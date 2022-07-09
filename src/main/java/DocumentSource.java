//import model.Document;
//import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Iterator;
//
//public class DocumentSource extends RichSourceFunction<Document> {
//    private Boolean isRunning = true;
//    private String path;
//    private InputStream dstream = null;
//
//    public DocumentSource(String path) {
//        this.path = path;
//    }
//
//    @Override
//    public void run(SourceContext<Document> ctx) throws Exception {
//        InputStream dstream = DocumentSource.class.getResourceAsStream(path);
////        Iterator<String> lines = Source
//        String line;
//        while (isRunning && (line = dstream.readLine()) != null){
//            String[] es = line.split("\t");
//            String[] keywords = es[2].substring(1, es[2].length() - 1).split(",");
//            ctx.collect(new Document(es[0],es[1], new ArrayList<>(Arrays.asList(keywords))));
//        }
//    }
//
//    @Override
//    public void cancel() {
//        dstream.close();
//        isRunning = false;
//    }
//}
