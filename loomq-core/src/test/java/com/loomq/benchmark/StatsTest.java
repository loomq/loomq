package com.loomq.benchmark;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class StatsTest {

    @Test void medianEvenCount() {
        assertEquals(2.5, Stats.median(Stats.sorted(new double[]{1, 2, 3, 4})));
    }
    @Test void medianOddCount() {
        assertEquals(3.0, Stats.median(Stats.sorted(new double[]{1, 2, 3, 4, 5})));
    }
    @Test void quartilesAndIqr() {
        double[] s = Stats.sorted(new double[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        // linear-interp 25%: pos=0.25*9=2.25 -> sorted[2]+0.25*(sorted[3]-sorted[2]) = 3+0.25*1 = 3.25
        assertEquals(3.25, Stats.q1(s));
        // 75%: pos=0.75*9=6.75 -> sorted[6]+0.75*(sorted[7]-sorted[6]) = 7+0.75*1 = 7.75
        assertEquals(7.75, Stats.q3(s));
        assertEquals(4.5, Stats.iqr(s));
    }
    @Test void p99Robustish() {
        double[] s = Stats.sorted(new double[]{10, 10, 10, 10, 10, 10, 10, 10, 10, 1000});
        // pos=0.99*9=8.91 -> sorted[8]+0.91*(sorted[9]-sorted[8]) = 10+0.91*990 = 910.9
        assertEquals(910.9, Stats.p99(s), 0.001);
    }
    @Test void emptyThrows() {
        assertThrows(IllegalArgumentException.class, () -> Stats.median(new double[0]));
    }
}