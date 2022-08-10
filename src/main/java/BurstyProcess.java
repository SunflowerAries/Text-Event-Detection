import lib.BurstyProb;
import model.Feature;
import model.FeatureOccurrence;
import model.PerDayInfo;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BurstyProcess extends RichWindowFunction<FeatureOccurrence, Feature, String,TimeWindow> {
    private MapState<String, Integer> date2DocNum;
    private MapState<String, Feature> word2Feature;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private int coldDownDays = 7;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> date2DocNumDes =
                new MapStateDescriptor<String, Integer>(
                        "date2DocNum",
                        String.class,
                        Integer.class
                );
        MapStateDescriptor<String, Feature> word2FeatureDes =
                new MapStateDescriptor<String, Feature>(
                        "date2WordsDes",
                        String.class,
                        Feature.class
                );

        date2DocNum = getRuntimeContext().getMapState(date2DocNumDes);
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
        Integer docNum = date2DocNum.get(date);
        if(docNum != null){
            System.err.println("previous " + date + " has existed!!!");
//            throw new RuntimeException();
            return;
        }
        date2DocNum.put(date, N);
        coldDownDays--;
        for (String w: w2FO.keySet()){ // all words in current date
            // update map
            Feature f = word2Feature.get(w);
            if(f == null) f = new Feature(w);
            HashSet<String> ds = word2DocList.get(w);
            if (f.occurrence.size() == 7) {
                String minus7 = LocalDate.parse(date, formatter).minusDays(7).toString();
                f.occurrence.remove(minus7);
            }
            f.occurrence.put(date, new PerDayInfo(ds));
            word2Feature.put(w, f);

            // bursty features judgement based on Feature object
            int n = ds.size();
            int avgDocNum = (int) StreamSupport.stream(date2DocNum.values().spliterator(),true).mapToInt(Integer::intValue).average().getAsDouble();
            double p = f.get_p(date2DocNum.entries());

            // cold down to get enough static data
            if (coldDownDays > 0) continue;
            f.occurrence.get(date).setScore(BurstyProb.calc(N, n, p));
            if (!f.isStopword(avgDocNum, p) && f.occurrence.get(date).getScore() > 1e-6){
                out.collect(f);
            }
        }
    }
}
