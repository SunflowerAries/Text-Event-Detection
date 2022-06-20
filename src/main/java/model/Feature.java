package model;

import java.util.HashMap;
import java.util.Set;

public class Feature {
    public String word;
    public HashMap<String, Set<String>> occurrence;

    public Feature() {
        this.word = null;
        this.occurrence = new HashMap<>();
    }

    public Feature(String word, HashMap<String, Set<String>> occurrence) {
        this.word = word;
        this.occurrence = occurrence;
    }
}