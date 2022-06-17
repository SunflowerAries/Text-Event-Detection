import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"newsdesk", "section", "subsection", "material",
        "headline", "abstract", "keywords", "word_count",
        "pub_date", "n_comments", "uniqueID"})
public class NYTPojo {
    public String newsdesk;
    public String section;
    public String subsection;
    public String material;
    public String headline;
    public String abstract0;
    public String keywords;
    public String word_count;
    public String pub_date;
    public String n_comments;
    public String uniqueID;
}
