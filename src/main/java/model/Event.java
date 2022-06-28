package model;

import java.util.ArrayList;
import java.util.List;

public class Event {
    public List<String> features;

    public Event(String feature) {
        this.features = new ArrayList<String>();
        this.features.add(feature);
    }

    public Event(List<String> features) {
        this.features = features;
    }

    @Override
    public String toString() {
        return "Event{" +
                "features=" + features +
                '}';
    }
}