import model.Event;
import model.Feature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class FeatureEventMapper implements FlatMapFunction<Map<String, Feature>, List<Event>> {

    @Override
    public void flatMap(Map<String, Feature> featureMap, Collector<List<Event>> collector) throws Exception {
        List<Event> events = new ArrayList<Event>();
        Set<String> words = new HashSet<>(featureMap.keySet());
        while(!words.isEmpty()) {
            Iterator<String> iterator = words.iterator();
            String word = iterator.next();
            iterator.remove();
            Event event = new Event(word);

            for (int i = 0; i < 5 && !words.isEmpty(); i++) {
                Iterator<String> it = words.iterator();
                while (it.hasNext()) {
                    String candidate = it.next();
                    double score = evaluate(event, candidate, featureMap);
                    if (score >= 0) {
                        event.features.add(candidate);
                        it.remove();
                        break;
                    }
                }
            }
            events.add(event);
        }
        collector.collect(events);
        return;
    }

    public double evaluate(Event event, String candidate, Map<String, Feature> featureMap) {
        Set<String> union = new HashSet<>();
        Set<String> inter = new HashSet<>(featureMap.get(event.features.get(0)).documents);

        event.features.forEach((String f) -> {
            union.addAll(featureMap.get(f).documents);
            inter.retainAll(featureMap.get(f).documents);
        });
        double origin = (double)(inter.size()) / union.size();

        union.addAll(featureMap.get(candidate).documents);
        inter.retainAll(featureMap.get(candidate).documents);
        double after = (double)(inter.size()) / union.size();

        return after - origin;
    }
}
