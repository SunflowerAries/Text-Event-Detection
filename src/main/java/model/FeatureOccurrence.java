package model;

public class FeatureOccurrence {
    public String word;
    public String documentId;
    public String date;
    public long timeStamp;

    public FeatureOccurrence(String word, String documentId, String date, long timeStamp) {
        this.word = word;
        this.documentId = documentId;
        this.date = date;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "FeatureOccurrence{" +
                "word='" + word + '\'' +
                ", documentId='" + documentId + '\'' +
                ", date='" + date + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
