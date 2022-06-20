import model.Event;
import model.Feature;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

public class Feature2Event extends RichFlatMapFunction<Feature, Event> implements CheckpointedFunction {

    // for state management in flink https://juejin.cn/post/6844904053512601607
    private ListState<Feature> bursty;

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        bursty = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<Feature>("bursty", Feature.class));
    }

    @Override
    public void flatMap(Feature bur, Collector<Event> out) throws Exception {
        bursty.add(bur);

    }
}