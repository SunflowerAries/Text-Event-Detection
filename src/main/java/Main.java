import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


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

        // Keywords and datetime are filterd from the csv file
        // The text lines read from the source are split into words
        // using a user-defined function. The tokenizer, implemented below,
        // will output each word as a (2-tuple) containing (word, 1)
        DataStream<Bursty> bursty = news.flatMap(new Tokenizer())
                .name("tokenizer")
                .keyBy(value -> value.keyword)
                .process(new BurstyAggregateFunction())
                // https://blog.csdn.net/qq_37555071/article/details/122415430
                // for reference to global which means N->1
                .global();
        bursty.flatMap(new Feature2Event());


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
            implements FlatMapFunction<Row, FeatureOccurrence> {

        @Override
        public void flatMap(Row value, Collector<FeatureOccurrence> out) {

            String keywords = (String) value.getField(2);

            String[] keywordList = keywords.substring(1, keywords.length() - 1).split(",");
            String date = ((String) value.getField(0)).split(" ")[0];
            String id = (String) value.getField(1);
            for (String keyword : keywordList) {
                out.collect(new FeatureOccurrence(keyword, id, date));
            }
        }
    }

    public static final class Feature2Event
            extends RichFlatMapFunction<Bursty, Event> {

        private ListState<Bursty> bursty;

        @Override
        public void open(Configuration parameters) {
            bursty = getRuntimeContext().getListState(new ListStateDescriptor<Bursty>("myBursty", Bursty.class));
        }

        @Override
        public void flatMap(Bursty bur, Collector<Event> out) throws Exception {

            bursty.add(bur);

        }
    }

    public static final class BurstyAggregateFunction
        extends KeyedProcessFunction<String, FeatureOccurrence, Bursty> {

        private ValueState<BurstyWithTimeStamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<BurstyWithTimeStamp>("myState", BurstyWithTimeStamp.class));
        }

        // all the data will be kept in memory
        @Override
        public void processElement(
                FeatureOccurrence occurrence,
                Context ctx,
                Collector<Bursty> out) throws Exception {

            BurstyWithTimeStamp current = state.value();

            if (current == null) {
                current = new BurstyWithTimeStamp();
                current.keyword = occurrence.keyword;
            }

            if (current.occurrence.containsKey(occurrence.date)) {
                current.occurrence.get(occurrence.date).add(occurrence.documentId);
            } else {
                List<String> documentList = new ArrayList<>();
                documentList.add(occurrence.documentId);
                current.occurrence.put(occurrence.date, documentList);
            }

            current.lastModified = ctx.timestamp();
            state.update(current);
            long timer = current.lastModified + 10000;
            ctx.timerService().registerProcessingTimeTimer(timer);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Bursty> out) throws Exception {
            BurstyWithTimeStamp bur = state.value();
            if (timestamp == bur.lastModified + 10000) {
//                System.out.println(bur.keyword);
//                for (String date : bur.occurrence.keySet()) {
//                    System.out.println(date + ": " + String.join(", ", bur.occurrence.get(date)));
//                }
                out.collect(new Bursty(bur.keyword, bur.occurrence));
            }
        }
    }
}
