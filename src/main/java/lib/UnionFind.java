package lib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UnionFind {
    private int[] ufs;

    public UnionFind(int n) {
        ufs = new int[n];
        for (int i = 0; i < n; i++) ufs[i] = i;
    }

    public int find(int x) {
        return x == ufs[x] ? x : (ufs[x] = find(ufs[x]));
    }

    public void unite(int x, int y) {
        int u = find(x), v = find(y);
        ufs[v] = u;
    }

    public List<List<Integer>> getSet() {
        HashMap<Integer, List<Integer>> map = new HashMap<Integer, List<Integer>>();
        for (int i = 0; i < ufs.length; i++) {
            int fa = find(i);
            map.computeIfAbsent(fa, k -> new ArrayList<Integer>()).add(i);
        }
        List<List<Integer>> res = new ArrayList<>();
        for (int i : map.keySet()) {
            res.add(map.get(i));
        }
        return res;
    }
}
