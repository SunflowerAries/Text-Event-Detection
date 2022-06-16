import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.socketTextStream("localhost", 6666);

        DataStream<Tuple2<String, Integer>> result = input.flatMap(
            new FlatMapFunction<String, Tuple2<String, Integer>>() {
                public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    String[] words = s.split(" ");
                    for (String word: words) {
                        collector.collect(new Tuple2<String, Integer>(word, 1));
                    }
                }
            }
        ).keyBy(0).sum(1);

        result.print();

        env.execute();
    }
}
