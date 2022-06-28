import model.Event;
import model.Feature;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Feature2Event extends RichFlatMapFunction<Feature, Event> {

    // for state management in flink https://juejin.cn/post/6844904053512601607
    private MapState<String, Feature> burstyMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        burstyMap = getRuntimeContext().getMapState(new MapStateDescriptor<String, Feature>("burstyMap", String.class, Feature.class));
    }

    @Override
    public void flatMap(Feature bursty, Collector<Event> out) throws Exception {
        burstyMap.put(bursty.word, bursty);

        List<Event> events = new ArrayList<Event>();
        Set<String> candidates = new HashSet<String>();
        burstyMap.keys().forEach((String word) -> candidates.add(word));

        while(!candidates.isEmpty()) {
            Iterator<String> iterator = candidates.iterator();
            String candidate = iterator.next();
            iterator.remove();
            Event event = new Event(candidate);

            for (int i = 0; i < 5 && !candidates.isEmpty(); i++) {
                Iterator<String> it = candidates.iterator();
                while (it.hasNext()) {
                    candidate = it.next();
                    double score = evaluate(event, candidate);
                    if (score >= 0) {
                        event.features.add(candidate);
                        it.remove();
                        break;
                    }
                }
            }
            events.add(event);
        }

        Event hotEvent = hotPeriod(events);
        out.collect(hotEvent);
    }

    private double evaluate(Event event, String candidate) throws Exception {
        Set<String> union = new HashSet<>();
        Set<String> inter = new HashSet<>(burstyMap.get(event.features.get(0)).documents());

        event.features.forEach((String f) -> {
            try {
                union.addAll(burstyMap.get(f).documents());
                inter.retainAll(burstyMap.get(f).documents());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        double origin = event.features.size() == 1 ? 0 : (double)(inter.size()) / union.size();

        union.addAll(burstyMap.get(candidate).documents());
        inter.retainAll(burstyMap.get(candidate).documents());
        double after = (double)(inter.size()) / union.size();

        return after - origin;
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