package org.apache.kylin.common.util;

import java.util.Iterator;
import java.util.Queue;

/**
 * Created by Hongbin Ma(Binmahone) on 3/27/15.
 */
public class FIFOIterable<T> implements Iterable<T> {
    private Queue<T> q;

    public FIFOIterable(Queue<T> q) {
        this.q = q;
    }

    @Override
    public Iterator<T> iterator() {
        return new FIFOIterator<T>(q);
    }
}
