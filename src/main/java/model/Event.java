package model;

import java.util.ArrayList;
import java.util.List;

public class Event {
    public List<String> features;

    public Event() {
        this.features = new ArrayList<String>();
    }

    public Event(String feature) {
        this.features = new ArrayList<String>();
        this.features.add(feature);
    }
}
