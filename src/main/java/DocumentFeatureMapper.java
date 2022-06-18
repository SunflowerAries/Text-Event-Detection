import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import model.Feature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class DocumentFeatureMapper implements FlatMapFunction<Row, Map<String, Feature>> {

    public Map<String, Feature> featureMap = new HashMap<>();

    @Override
    public void flatMap(Row row, Collector<Map<String, Feature>> collector) throws Exception {
        String keywordsText = (String)row.getField(6);
        String date = ((String)row.getField(8)).split(" ")[0];
        String documentId = (String) row.getField(9);

        JSONArray keywords = new JSONArray();
        try {
            keywords = (JSONArray) JSON.parse(keywordsText);
        } catch (Exception e) {

        }

        keywords.stream().forEach(word -> {
            Feature feature = featureMap.getOrDefault(word, new Feature((String) word));
            feature.add(date, documentId);
            featureMap.put((String) word, feature);
        });
        collector.collect(featureMap);
    }
}
