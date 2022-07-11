import lib.BurstyProb;
import model.FeatureOccurrence;
import model.Feature;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BurstyProcess extends RichWindowFunction<FeatureOccurrence, Feature, String,TimeWindow> {
    private MapState<String, Integer> date2DocNum;
    private MapState<String, List<String>> date2Words;
    private MapState<String, Tuple2<String, Integer>> word2Pair; // Pair(date, doc_num)
    private MapState<String, List<String>> word2Docs;
    private MapState<String, Feature> word2Feature;

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
        MapStateDescriptor<String, Feature> word2FeatureDes =
                new MapStateDescriptor<String, Feature>(
                        "date2WordsDes",
                        String.class,
                        Feature.class
                );

        date2DocNum = getRuntimeContext().getMapState(date2DocNumDes);
        date2Words = getRuntimeContext().getMapState(date2WordsDes);
        word2Docs = getRuntimeContext().getMapState(wordsDocsDes);
        word2Feature = getRuntimeContext().getMapState(word2FeatureDes);
    }

    @Override
    public void apply(String s, TimeWindow window, Iterable<FeatureOccurrence> input, Collector<Feature> out) throws Exception {
        String date = null;
        HashSet<String> docs = new HashSet<>(); // docs for current date
        HashMap<String, HashSet<String>> word2DocList = new HashMap<>(); //word 2 doc_num in a date
        Map<String, List<FeatureOccurrence>> w2FO = StreamSupport.stream(input.spliterator(), true).collect(Collectors.groupingBy(FeatureOccurrence::getWord));
        for (Map.Entry<String, List<FeatureOccurrence>> entry:w2FO.entrySet()){
            HashSet<String> docSet = word2DocList.get(entry.getKey());
            if(docSet == null) docSet = new HashSet<>();
            List<String> curDocs = entry.getValue().stream().map(FeatureOccurrence::getDocumentId).collect(Collectors.toList());
            docSet.addAll(curDocs); // add all doc_id
            docs.addAll(curDocs);
            word2DocList.put(entry.getKey(), docSet);
            if(date == null) date = entry.getValue().get(0).date;
        }
        int N = docs.size();
        Integer dd = date2DocNum.get(date);
        if(dd != null){
            System.err.println("previous " + date + " is not null!!!");
        }
        date2DocNum.put(date, N);

        // TODO: cold down days
        for (String w: w2FO.keySet()){ // all words in current date
            // update map
            Feature f = word2Feature.get(w) ;
            if(f == null) f = new Feature(w);
            HashSet<String> ds = word2DocList.get(w);
            f.occurrence.put(date, new HashSet<>(ds));
            word2Feature.put(w, f);

            // bursty features judgement based on Feature object
            int n = ds.size();
            int avgDocNum = (int) StreamSupport.stream(date2DocNum.values().spliterator(),true).mapToInt(Integer::intValue).average().getAsDouble();
            double p = f.get_p(date2DocNum.entries()); // TODO: only window that contains feature

            if (!f.isStopword(avgDocNum, p) && BurstyProb.calc(N, n, p) > 1e-6){
                out.collect(f);
            }

        }


    }
}
