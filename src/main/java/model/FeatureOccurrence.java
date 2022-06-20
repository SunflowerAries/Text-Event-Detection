package model;

public class FeatureOccurrence {
    public String word;
    public String documentId;
    public String date;

    public FeatureOccurrence(String word, String id, String date) {
        this.word = word;
        this.documentId = id;
        this.date = date;
    }
}
