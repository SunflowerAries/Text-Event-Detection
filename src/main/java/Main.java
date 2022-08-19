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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Objects;

public class Main {
    public static ArrayList<Row> prepare(String filename) throws IOException {
        BufferedReader in = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(Main.class.getClassLoader().getResourceAsStream(filename))));

        ArrayList<Row> ds = new ArrayList<Row>();
        String line;
        while ((line = in.readLine()) != null)
        {
            String[] es = line.split("\t");
            Row row = new Row(3);
            row.setField(0, es[0]);
            row.setField(1, es[1]);
            row.setField(2, es[2]);
            ds.add(row);
        }
        return ds;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("taskmanager.memory.network.max", "1gb"); // insufficient buffer error (too many words as key)

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        String filename = "refine.csv";

        // InputStream way
        ArrayList<Row> ds = prepare(filename);
        DataStreamSource<Row> source = env.fromCollection(ds);

        // CSV file way: infeasible on jar
//        String inFilePath = Main.class.getResource(filename).getPath();
//        RowCsvInputFormat csvInput = new RowCsvInputFormat(new Path(inFilePath), new TypeInformation[]{
//                Types.STRING, Types.STRING, Types.STRING
//        }, "\n", "\t");
//        DataStreamSource<Row> source2 = env.readFile(csvInput, inFilePath);

        SingleOutputStreamOperator<FeatureOccurrence> middle = source
                .flatMap(new Document2Feature()).setParallelism(2)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<FeatureOccurrence>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<FeatureOccurrence>)(element, recordTimestamp) -> element.timeStamp))
                .name("Document->Feature");

        DataStream<Feature> bursty = middle.keyBy(b->"global_occur")
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new BurstyProcess()).name("BurstyProcess");

        bursty.keyBy(b -> "global_f2e").flatMap(new Feature2Event()).name("Feature->Event").print().name("Print");
        env.execute("BurstyEventDetection");
    }
}
