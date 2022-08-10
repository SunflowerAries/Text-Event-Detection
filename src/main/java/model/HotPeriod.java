package model;

import java.util.List;

public class HotPeriod extends Event {
    public String date;

    public HotPeriod(String date, List<String> features) {
        super(features);
        this.date = date;
    }

    @Override
    public String toString() {
        return "HotPeriod{" +
                "features=" + features +
                ", date=" + date +
                '}';
    }
}
