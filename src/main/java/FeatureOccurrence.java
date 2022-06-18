import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FeatureOccurrence {
    public String keyword;
    public String documentId;
    public String date;

    public FeatureOccurrence(String keyword, String id, String date) {
        this.keyword = keyword;
        this.documentId = id;
        this.date = date;
    }
}
