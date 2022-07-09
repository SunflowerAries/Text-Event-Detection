import model.Document;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class News2Doc implements MapFunction<Row, Document> {
    @Override
    public Document map(Row row) throws Exception {
        return null;
    }
}
