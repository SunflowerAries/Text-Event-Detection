import model.FeatureOccurrence;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class Document2Feature implements FlatMapFunction<Row, FeatureOccurrence> {

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