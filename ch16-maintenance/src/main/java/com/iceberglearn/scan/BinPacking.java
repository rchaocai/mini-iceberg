package com.iceberglearn.scan;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;

/**
 * First-fit bin-packing with a lookback window, mirroring real Iceberg's
 * {@code BinPacking.PackingIterator}.
 *
 * <p>Items are fed one at a time into a sliding window of at most {@code lookback}
 * open bins. For each item the packer scans the window front-to-back (oldest bin
 * first) and adds the item to the first bin that still has room (first-fit). If no
 * windowed bin can take it, a new bin is opened at the tail; when that pushes the
 * window past {@code lookback}, the <em>largest</em> bin is closed and emitted,
 * keeping the smaller bins around to absorb later items. When the input ends, the
 * remaining bins are drained front-to-back.
 *
 * <p>Compared with next-fit (which only ever inspects the last bin), this fills
 * bins more densely because a small item can still land in an earlier bin that has
 * room; and by closing the largest bin on overflow it releases the most complete
 * bin for execution first.
 */
final class BinPacking {

    private BinPacking() {
    }

    /**
     * Pack {@code items} into bins of at most {@code targetWeight} each, using a
     * lookback window of {@code lookback} bins and first-fit placement with
     * largest-bin eviction. Returns the bins in the order they were closed:
     * evicted bins first (largest-first at each overflow), then the surviving
     * bins drained oldest-first.
     */
    static <T> List<List<T>> pack(
            List<T> items, long targetWeight, int lookback, Function<T, Long> weightFunc) {
        Deque<Bin<T>> bins = new ArrayDeque<>();
        List<List<T>> packed = new ArrayList<>();
        for (T item : items) {
            long weight = weightFunc.apply(item);
            Bin<T> bin = findBin(bins, weight);
            if (bin != null) {
                bin.add(item, weight);
            } else {
                Bin<T> newBin = new Bin<>(targetWeight);
                newBin.add(item, weight);
                bins.addLast(newBin);
                if (bins.size() > lookback) {
                    packed.add(removeLargest(bins).items());
                }
            }
        }
        while (!bins.isEmpty()) {
            packed.add(bins.removeFirst().items());
        }
        return packed;
    }

    /** First-fit: scan the window oldest-to-newest, return the first bin that fits. */
    private static <T> Bin<T> findBin(Deque<Bin<T>> bins, long weight) {
        for (Bin<T> bin : bins) {
            if (bin.canAdd(weight)) {
                return bin;
            }
        }
        return null;
    }

    /** Remove and return the heaviest bin in the window (O(n) in the window size). */
    private static <T> Bin<T> removeLargest(Collection<Bin<T>> bins) {
        Bin<T> max = Collections.max(bins, Comparator.comparingLong(Bin::weight));
        bins.remove(max);
        return max;
    }

    private static final class Bin<T> {
        private final long targetWeight;
        private final List<T> items = new ArrayList<>();
        private long binWeight;

        Bin(long targetWeight) {
            this.targetWeight = targetWeight;
        }

        boolean canAdd(long weight) {
            return binWeight + weight <= targetWeight;
        }

        void add(T item, long weight) {
            this.binWeight += weight;
            items.add(item);
        }

        long weight() {
            return binWeight;
        }

        List<T> items() {
            return items;
        }
    }
}
