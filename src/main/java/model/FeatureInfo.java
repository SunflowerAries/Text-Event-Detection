package model;

import javafx.util.Pair;

import java.util.HashMap;

public class FeatureInfo {
    private String word;
    private HashMap<String, Integer> date2DocNum;


    public static class Info {
        private String _date;
        private BitSet _doc_set;
        private Pair<Integer, Integer> _doc_info; // （doc_num that contain feature f, doc_num in a window)

        public Info(String date, BitSet doc_set, Pair<Integer, Integer> doc_info) {
            _date = date;
            _doc_set = doc_set;
            _doc_info = doc_info;
        }

        public String get_date() {
            return _date;
        }
        public BitSet get_doc_set() {
            return _doc_set;
        }
        public Pair<Integer, Integer> get_doc_info() {
            return _doc_info;
        }
        public Double get_p() {
            return Double.valueOf(_doc_info.getKey()) / _doc_info.getValue();
        }
    }

    private Feature _feature;
    private Info[] _infos;
    private int avgN;
    private double avgp;  //  p = doc num contain feature f / doc num in window

    public FeatureInfo(Feature feature, LinkedBlockingQueue<Info> infos) {
        _feature = feature;
        _infos = infos.toArray(new Info[0]);

        avgN = 0;
        avgp = 0;
        for (Info info : _infos) {  // all windows statistic ?
            avgN += info.get_doc_info().getValue(); // pair's second element, doc num in window
            avgp += Double.valueOf(info.get_doc_info().getKey()) / info.get_doc_info().getValue();
        }
        avgN /= _infos.length;
        avgp /= _infos.length;
    }

    public Feature get_feature() {
        return _feature;
    }
    public Info[] get_infos() {
        return _infos;
    }
    public void set_N() {
        avgN = 1;
    }
    public double get_p() {
        return avgp;
    }

    public boolean isStopword() {
        if (_infos.length <= 7) return false;  // when num window which contains feature if low
        boolean res = Binomial.binomial(avgN, avgN, avgp) > 0; // TODO: binomial > 0
        return res;
    }

}