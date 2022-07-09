package model;

import scala.collection.immutable.Stream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Feature {
    public String word;
    public HashSet<String> docList;

    public Feature() {
        this.word = null;
        this.docList = new HashSet<>();
    }

    public Feature(String word, HashSet<String> docList) {
        this.word = word;
        this.docList = docList;
    }

    public Set<String> documents() {
        return docList;
    }


    public int count() {
//        int count = occurrence.values().stream().mapToInt(Set::size).sum();
        return docList.size();
    }
}