import model.Feature;
//import model.FeatureInfo;
import model.FeatureOccurrence;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.*;

public class BurstyProcess extends RichWindowFunction<FeatureOccurrence, Feature, String,TimeWindow> {
    private MapState<String, Integer> date2DocNum;
    private MapState<String, List<String>> date2Words;
    private MapState<String, Tuple2<String, Integer>> word2Pair; // Pair(date, doc_num)
    private MapState<String, List<String>> word2Docs;

    //    private MapState<String, FeatureInfo> word2Info;
    private ValueState<String> curDate; // current date

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> date2DocNumDes =
                new MapStateDescriptor<String, Integer>(
                        "date2DocNum",
                        String.class,
                        Integer.class
                );
        MapStateDescriptor<String, List<String>> date2WordsDes =
                new MapStateDescriptor<String, List<String>>(
                        "date2WordsDes",
                        Types.STRING,
                        Types.LIST(Types.STRING)
                );
        MapStateDescriptor<String, List<String>> wordsDocsDes =
                new MapStateDescriptor<String, List<String>>(
                        "date2WordsDes",
                        Types.STRING,
                        Types.LIST(Types.STRING)
                );

        date2DocNum = getRuntimeContext().getMapState(date2DocNumDes);
        date2Words = getRuntimeContext().getMapState(date2WordsDes);
        word2Docs = getRuntimeContext().getMapState(wordsDocsDes);
    }

    @Override
    public void apply(String s, TimeWindow window, Iterable<FeatureOccurrence> input, Collector<Feature> out) throws Exception {
        String date = null;
        HashSet<String> words = new HashSet<>(); // words in a date
        HashSet<String> docs = new HashSet<>();
        HashMap<String, HashSet<String>> word2DocList = new HashMap<>(); //word 2 doc_num in a date
        for (FeatureOccurrence fo: input){
            words.add(fo.word);
            docs.add(fo.documentId);

            HashSet<String> docSet = word2DocList.get(fo.word);
            docs.add(fo.documentId);
            word2DocList.put(fo.word, docSet);

        }
        int N = docs.size();

        for (String w: words){
            // bursty features judgement



            // update map
            List<String> docsOfWord = word2Docs.get(w);
            docsOfWord.addAll(word2DocList.get(w));
            word2Docs.put(w, docsOfWord);
        }

        Integer dd = date2DocNum.get(date);
        if(dd != null){
            System.err.println(date + " is not null!!!");
        }
        date2DocNum.put(date, N);
    }
}
