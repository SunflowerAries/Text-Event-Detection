import model.Feature;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;


public class Main {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        String inFilePath = "../data/refine.csv";

        TypeInformation[] fieldTypes = new TypeInformation[]{
                Types.STRING, Types.STRING, Types.STRING
        };
        RowCsvInputFormat csvInput = new RowCsvInputFormat(new Path(inFilePath), fieldTypes, "\n", "\t");
        DataStreamSource<Row> news = env.readFile(csvInput, inFilePath);

        DataStream<Feature> bursty = news.flatMap(new Document2Feature())
                .name("document->feature")
                .keyBy(value -> value.word)
                .process(new BurstyAggregate())
                .name("burstyaggregate")
                // https://blog.csdn.net/qq_37555071/article/details/122415430
                // for reference to global which means N->1
                .global();
        bursty.flatMap(new Feature2Event());

        env.execute("BurstyEventDetection");
    }
}
