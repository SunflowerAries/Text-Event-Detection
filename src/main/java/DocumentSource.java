import model.Document;
import model.Feature;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

public class DocumentSource implements SourceFunction<Document> {

    public void run(SourceContext<Document> sourceContext) throws Exception {
        while (true) {
            Document document = new Document();
            sourceContext.collect(document);
            Thread.sleep(1000);
        }
    }

    public void cancel() {

    }
}
