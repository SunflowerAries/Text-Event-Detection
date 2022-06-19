import java.util.HashMap;
import java.util.List;

public class Bursty {
    public String keyword;
    public HashMap<String, List<String>> occurrence;

    public Bursty() {
        this.keyword = null;
        this.occurrence = new HashMap<>();
    }

    public Bursty(String keyword, HashMap<String, List<String>> occurrence) {
        this.keyword = keyword;
        this.occurrence = occurrence;
    }
}