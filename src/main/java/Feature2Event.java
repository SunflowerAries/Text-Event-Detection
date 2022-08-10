import lib.UnionFind;
import model.Event;
import model.Feature;
import model.HotPeriod;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Feature2Event extends RichFlatMapFunction<Feature, HotPeriod> {

    // for state management in flink https://juejin.cn/post/6844904053512601607
    private MapState<String, Feature> burstyMap;
    private MapState<String, Boolean> hotEvents;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private double hotPeriodThreshold = 1e-4;
    private static String filePath = "hotperiod";

    @Override
    public void open(Configuration parameters) throws Exception {
        burstyMap = getRuntimeContext().getMapState(new MapStateDescriptor<String, Feature>("burstyMap", String.class, Feature.class));
        hotEvents = getRuntimeContext().getMapState(new MapStateDescriptor<String, Boolean>("hotEvent", String.class, Boolean.class));
    }

    @Override
    public void flatMap(Feature bursty, Collector<HotPeriod> out) throws Exception {
        burstyMap.put(bursty.word, bursty);

        List<Event> events = new ArrayList<Event>();
        List<String> candidates = new ArrayList<String>();
        burstyMap.keys().forEach(candidates::add);

        int n = candidates.size();
        UnionFind union = new UnionFind(n);
        for (int i = 0; i < n; i++)
            for (int j = i + 1; j < n; j++) {
                if (scoreCompare(candidates.get(i), candidates.get(j))) {
                    union.unite(i, j);
                }
            }
        union.getSet().forEach(burstycluster -> {
            List<String> cluster = new ArrayList<>();
            burstycluster.forEach(bIndex -> cluster.add(candidates.get(bIndex)));
            Event event = new Event(cluster);
            events.add(event);
        });

//        Event hotEvent = hotPeriod(events);
        for (Event event : events) {
            try {
                if (!hotEvents.contains(event.toString())) {
                    hotEvents.put(event.toString(), true);
                    if (event.features.size() > 1) {
                        // bursty feature
                        String startTime = "2020-12-31", endTime = "2020-10-01";
                        for (String feature : event.features) {
                            try {
                                // keyset date
                                for (String date : burstyMap.get(feature).occurrence.keySet()) {
                                    if (date.compareTo(startTime) < 0)
                                        startTime = date;
                                    if (date.compareTo(endTime) > 0)
                                        endTime = date;
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        String date = startTime;
                        double score = 0, tmpscore = 0;
                        while (!date.equals(endTime)) {
                            for (String feature : event.features) {
                                try {
                                    if (burstyMap.get(feature).occurrence.get(date) != null)
                                        tmpscore = burstyMap.get(feature).occurrence.get(date).getScore();
                                    else
                                        tmpscore = 0;
                                    score += tmpscore;
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            if (score / event.features.size() > hotPeriodThreshold)
                                out.collect(new HotPeriod(date, event.features));
                            date = LocalDate.parse(date, formatter).plusDays(1).toString();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean scoreCompare(String candidate1, String candidate2) throws Exception {
        HashSet<String> dates1 = new HashSet<>(burstyMap.get(candidate1).occurrence.keySet());
        HashSet<String> dates2 = new HashSet<>(burstyMap.get(candidate2).occurrence.keySet());
        int a = dates1.size();
        int b = dates2.size();
        dates1.addAll(dates2);
        int c = dates1.size();
        double Pe1 = (double)(a + b - c) / c, Pe2 = (double) 1;

        a = b = c = 0;
        int tmpb;
        for (String date : burstyMap.get(candidate1).occurrence.keySet()) {
            a += burstyMap.get(candidate1).occurrence.get(date).getIds().size();
            tmpb = burstyMap.get(candidate2).occurrence.containsKey(date) ? burstyMap.get(candidate2).occurrence.get(date).getIds().size() : 0;
            if (tmpb == 0) {
                c += a;
            } else {
                b += tmpb;
                Set<String> C = new HashSet<>(burstyMap.get(candidate1).occurrence.get(date).getIds());
                C.addAll(burstyMap.get(candidate2).occurrence.get(date).getIds());
                c += C.size();
            }
        }

        double Pde1 = (double) a / c * b / c, Pde21 = (1 - (double) a / c) * b / c, Pde22 = (1 - (double) b / c) * a / c;
        double score1 = Pde1 * Pe1, score21 = Pde21 * Pe2, score22 = Pde22 * Pe2;
        return score1 > score21 && score1 > score22;
    }

    private Event hotPeriod(List<Event> events) {
        Map<Event, Integer> eventFreq = new HashMap<>();
        events.forEach(event -> {
            int count = event.features.stream().mapToInt(bursty -> {
                try {
                    return burstyMap.get(bursty).count();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            }).sum();
            eventFreq.put(event, count);
        });

        List<Map.Entry<Event, Integer>> sortedEventFreq = new ArrayList<>(eventFreq.entrySet());
        Collections.sort(sortedEventFreq, new Comparator<Map.Entry<Event, Integer>>() {
            @Override
            public int compare(Map.Entry<Event, Integer> o1, Map.Entry<Event, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        return sortedEventFreq.get(0).getKey();
    }
}