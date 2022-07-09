package model;

import java.util.ArrayList;
import java.util.List;

public class Document {
    private String date;
    private String doc_id;
    private ArrayList<String> words;

    public Document() {
    }
    public Document(String date, String doc_id, ArrayList<String> words) {
        this.date = date;
        this.doc_id = doc_id;
        this.words = words;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getDoc_id() {
        return doc_id;
    }

    public void setDoc_id(String doc_id) {
        this.doc_id = doc_id;
    }

    public ArrayList<String> getWords() {
        return words;
    }

    public void setWords(ArrayList<String> words) {
        this.words = words;
    }

    @Override
    public String toString() {
        return "Document{" +
                "date='" + date + '\'' +
                ", doc_id='" + doc_id + '\'' +
                '}';
    }
}
