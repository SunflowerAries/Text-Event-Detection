import model.Document;
import model.Event;
import model.Feature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.stream.Collector;

public class BurstyDetection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TypeInformation[] fieldTypes = new TypeInformation[]{
                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
        };
        RowCsvInputFormat csvInput = new RowCsvInputFormat(null, fieldTypes);
        csvInput.setSkipFirstLineAsHeader(true);
        csvInput.enableQuotedStringParsing('"');
        DataStreamSource<Row> news = env.readFile(csvInput, "src/main/resources/test.csv");

        SingleOutputStreamOperator<List<Event>> result = news.flatMap(new DocumentFeatureMapper())
                .flatMap(new FeatureEventMapper());

        result.print();

        env.execute();
    }

}
