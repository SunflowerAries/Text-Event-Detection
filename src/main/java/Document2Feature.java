import model.FeatureOccurrence;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class Document2Feature implements FlatMapFunction<Row, FeatureOccurrence> {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.US);
    @Override
    public void flatMap(Row value, Collector<FeatureOccurrence> out) {

        String keywords = (String) value.getField(2);

        String[] keywordList = keywords.substring(1, keywords.length() - 1).split(",");
        String date = ((String) value.getField(0)).split(" ")[0];

        LocalDateTime dateTime = LocalDate.parse(date, DATE_TIME_FORMATTER).atStartOfDay();

        long timeStamp = dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();

        String id = (String) value.getField(1); // doc_id
        if(date.length() == 0) {

            System.err.println("date of" + id);
        }
        for (String keyword : keywordList) {
            out.collect(new FeatureOccurrence(keyword, id, date, timeStamp));
        }
    }
}