import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

class FeaturePerDay {
    public List<String> documentid;
    public String date;

    public FeaturePerDay(String id, String date) {
        this.documentid = new ArrayList<>();
        this.documentid.add(id);
        this.date = date;
    }
}

public class Bursty {
    public String keyword;
    public HashMap<String, List<String>> occurrence;

    public Bursty() {
        this.keyword = null;
        this.occurrence = new HashMap<>();
    }

    public Bursty(String keyword, String id, String date) {
        this.keyword = keyword;
        this.occurList = new ArrayList<>();
        FeaturePerDay occ = new FeaturePerDay(id, date);
        occurList.add(occ);
    }
}
