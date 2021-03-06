import model.Feature;
import model.FeatureOccurrence;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Main {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("taskmanager.memory.network.max", "1gb"); // insufficient buffer error (too many words as key)

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        String inFilePath = Main.class.getResource("refine.csv").getPath();

        RowCsvInputFormat csvInput = new RowCsvInputFormat(new Path(inFilePath), new TypeInformation[]{
                Types.STRING, Types.STRING, Types.STRING
        }, "\n", "\t");

        SingleOutputStreamOperator<FeatureOccurrence> source = env.readFile(csvInput, inFilePath)
                .flatMap(new Document2Feature())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<FeatureOccurrence>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<FeatureOccurrence>)(element, recordTimestamp) -> element.timeStamp))
                .name("document->feature");

        DataStream<Feature> bursty = source.keyBy(b->"global_occur")
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new BurstyProcess()).name("BurstyProcess");

        bursty.keyBy(b -> "global_f2e").flatMap(new Feature2Event()).print();
        env.execute("BurstyEventDetection");
    }
}
