package model;

import lib.Binomial;

import java.util.*;

public class Feature {
    public String word;
    public HashMap<String, PerDayInfo> occurrence; // (date, documents<doc_id>)

    public Feature() {
        this.word = null;
        this.occurrence = new HashMap<>();
    }
    public Feature(String word) {
        this.word = word;
        this.occurrence = new HashMap<>();
    }
    public Feature(String word, HashMap<String, PerDayInfo> occurrence) {
        this.word = word;
        this.occurrence = occurrence;
    }
    public Double get_p(Iterable<Map.Entry<String, Integer>> iter) {
        double p = 0.0;
        int i = 0;
        for(Map.Entry<String,Integer> e:iter){
            if(occurrence.get(e.getKey()) == null) continue;
            p += occurrence.get(e.getKey()).ids.size() * 1.0 / e.getValue();
            i++;
        }
        assert occurrence.size() == i;
        return p / occurrence.size();
    }
    public boolean isStopword(int avgN, double p) {
        if (occurrence.size() <= 7) return false;  // when num window which contains feature if low
        boolean res = Binomial.binomial(avgN, avgN, p) > 0; // TODO: binomial > 0
        return res;
    }
    public Set<String> documents() {
        Set<String> documents = new HashSet<>();
        occurrence.values().forEach(ds -> {
            ds.ids.forEach(d -> documents.add(d));
        });
        return documents;
    }

    public int count(List<String> window) {// window is a list of date
        int count = (int) occurrence.entrySet().stream()
                .filter(entry -> window.contains(entry.getKey()))
                .flatMap(entry -> entry.getValue().ids.stream())
                .count();
        return count;
    }

    public int count() {
        int count = occurrence.values().stream().map(info -> info.ids).mapToInt(Set::size).sum();
        return count;
    }
}