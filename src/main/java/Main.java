import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        final CLI params = CLI.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(params.getExecutionMode());

        env.getConfig().setGlobalJobParameters(params);
        String inFilePath = "../data/test.csv";

        TypeInformation[] fieldTypes = new TypeInformation[]{
                Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                Types.STRING, Types.STRING, Types.STRING,
        };
        RowCsvInputFormat csvInput = new RowCsvInputFormat(new Path(inFilePath), fieldTypes, "\n", ",");
        csvInput.setSkipFirstLineAsHeader(true);
        csvInput.enableQuotedStringParsing('"');
        DataStreamSource<Row> news = env.readFile(csvInput, inFilePath);

        DataStream<Tuple3<String, List<String>, String>> counts =
                // Keywords and datetime are filterd from the csv file
                // The text lines read from the source are split into words
                // using a user-defined function. The tokenizer, implemented below,
                // will output each word as a (2-tuple) containing (word, 1)
                news.flatMap(new Tokenizer())
                        .name("tokenizer")
                        // keyBy groups tuples based on the "0" field, the word.
                        // Using a keyBy allows performing aggregations and other
                        // stateful transformations over data on a per-key basis.
                        // This is similar to a GROUP BY clause in a SQL query.
                        .keyBy(value -> value.f0)
                        // For each key, we perform a simple sum of the "1" field, the count.
                        // If the input data stream is bounded, sum will output a final count for
                        // each word. If it is unbounded, it will continuously output updates
                        // each time it sees a new instance of each word in the stream.
                        .sum(1)
                        .name("counter");

        if (params.getOutput().isPresent()) {
            // Given an output directory, Flink will write the results to a file
            // using a simple string encoding. In a production environment, this might
            // be something more structured like CSV, Avro, JSON, or Parquet.
            counts.sinkTo(
                            FileSink.<Tuple3<String, List<String>, String>>forRowFormat(
                                            params.getOutput().get(), new SimpleStringEncoder<>())
                                    .withRollingPolicy(
                                            DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                    .withRolloverInterval(Duration.ofSeconds(10))
                                                    .build())
                                    .build())
                    .name("file-sink");
        } else {
            counts.print().name("print-sink");
        }

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
        env.execute("WordCount");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<Row, Tuple3<String, List<String>, String>> {

        @Override
        public void flatMap(Row value, Collector<Tuple3<String, List<String>, String>> out) {

            String keywords = (String) value.getField(6);

            String[] keywordList = keywords.substring(1, keywords.length() - 1).replace("'", "").split(",");
            String date = ((String) value.getField(8)).split(" ")[0];
            for (String keyword : keywordList) {
                for (String token : keyword.split(" ")) {
                    out.collect(new Tuple3<>(token, List.of()));
                }
            }
            // normalize and split the line
//            String[] tokens = value.toLowerCase().split("\\W+");
//
//            // emit the pairs
//            for (String token : tokens) {
//                if (token.length() > 0) {
//                    out.collect(new Tuple2<>(token, 1));
//                }
//            }
        }
    }
}
