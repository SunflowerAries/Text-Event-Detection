package model;

import java.util.HashSet;
import java.util.Set;

public class PerDayInfo {
    double score;
    Set<String> ids;

    public Set<String> getIds() {
        return ids;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public double getScore() {
        return score;
    }

    public PerDayInfo(HashSet<String> ids) {
        this.score = 0;
        this.ids = ids;
    }

    public PerDayInfo() {
        this.score = 0;
        this.ids = new HashSet<>();
    }
}
