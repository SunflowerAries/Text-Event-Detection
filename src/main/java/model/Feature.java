package model;

import java.util.*;

public class Feature {
    public String word;
    public Map<String, Set<String>> occurrence;
    public Set<String> documents;

    public Feature(String word) {
        this.word = word;
        this.occurrence = new HashMap<String, Set<String>>();
        this.documents = new HashSet<>();
    }

    public void add(String date, String documentId) {
        Set<String> documentList = occurrence.getOrDefault(date, new HashSet<>());
        documentList.add(documentId);
        occurrence.put(date, documentList);
        documents.add(documentId);
    }
}
