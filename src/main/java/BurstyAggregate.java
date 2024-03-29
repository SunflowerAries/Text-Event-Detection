import model.Feature;
import model.FeatureOccurrence;
import model.FeatureWithTimeStamp;
import model.PerDayInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class BurstyAggregate extends KeyedProcessFunction<String, FeatureOccurrence, Feature> {

    private ValueState<FeatureWithTimeStamp> state; // state for a word

    int LIMIT = 15;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", FeatureWithTimeStamp.class));
    }

    // all the data will be kept in memory
    @Override
    public void processElement(
            FeatureOccurrence foccur,
            Context ctx,
            Collector<Feature> out) throws Exception {

        FeatureWithTimeStamp current = state.value();

        if (current == null) {
            current = new FeatureWithTimeStamp();
            current.word = foccur.word;
        }

        // https://stackoverflow.com/questions/66925393/are-flink-stream-messages-sent-to-downstream-in-order
        if (!current.occurrence.containsKey(foccur.date)) { // not in order
            current.occurrence.put(foccur.date, new PerDayInfo());
        }
        current.occurrence.get(foccur.date).getIds().add(foccur.documentId);

        current.lastModified = ctx.timestamp();
        state.update(current);
        long timer = current.lastModified + 1000; // register a timer, call after 1 second
        ctx.timerService().registerProcessingTimeTimer(timer);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Feature> out) throws Exception {
        FeatureWithTimeStamp bur = state.value();
        if (bur.occurrence.size() > LIMIT) {
            out.collect(new Feature(bur.word, bur.occurrence));
        }
        if (timestamp == bur.lastModified + 1000) {
//            System.out.println(bur.word);
//            for (String date : bur.occurrence.keySet()) {
//                System.out.println(date + ": " + String.join(", ", bur.occurrence.get(date)));
//            }
//            out.collect(new Feature(bur.word, bur.occurrence));
        }else{
            System.err.println("timer not equal");
        }
    }
}