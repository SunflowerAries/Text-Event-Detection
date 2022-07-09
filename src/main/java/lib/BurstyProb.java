package lib;

public class BurstyProb {

    public static double calc(int N, int k, double p) {
        // threshold == 1e-6
        double L = (N + 1) * p;  // x for binomial distribution reaches max value
        if (k <= L) return 0;  // region A in original paper
        int l = (int) Math.round(L), r = N;
        while (l <= r) {  // region B
            int mid = (l + r) / 2;
            if (Binomial.binomial(N, mid, p) > 1e-13) {
                l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        double R = r;
        if (k > R) return 1;  // region C
        return 1.0 / (1 + Math.exp(-(k - Math.round((L + R) / 2)))); // sigmoid
    }

}
