package model;

import scala.collection.immutable.Stream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FeatureOld {
    public String word;
    public HashMap<String, Set<String>> occurrence; // (date, documents<doc_id>)

    public FeatureOld() {
        this.word = null;
        this.occurrence = new HashMap<>();
    }

    public FeatureOld(String word, HashMap<String, Set<String>> occurrence) {
        this.word = word;
        this.occurrence = occurrence;
    }

    public Set<String> documents() {
        Set<String> documents = new HashSet<>();
        occurrence.values().forEach((Set<String> ds) -> {
            ds.forEach(d -> documents.add(d));
        });
        return documents;
    }

    public int count(List<String> window) {// window is a list of date
        int count = (int) occurrence.entrySet().stream()
                .filter(entry -> window.contains(entry.getKey()))
                .flatMap(entry -> entry.getValue().stream())
                .count();
        return count;
    }

    public int count() {
        int count = occurrence.values().stream().mapToInt(Set::size).sum();
        return count;
    }
}