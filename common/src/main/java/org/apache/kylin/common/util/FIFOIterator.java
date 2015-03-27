package org.apache.kylin.common.util;

import java.util.Iterator;
import java.util.Queue;

/**
 * Created by Hongbin Ma(Binmahone) on 3/27/15.
 *
 * Normal iterators in Collections are fail-safe,
 * i.e. adding elements to a queue will break current iterator.
 * The FIFOIterator is stateless, it only check the first element of a Queue
 */
public class FIFOIterator<T> implements Iterator<T> {
    private Queue<T> q;

    public FIFOIterator(Queue<T> q) {
        this.q = q;
    }

    @Override
    public boolean hasNext() {
        return !q.isEmpty();
    }

    @Override
    public T next() {
        return q.poll();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
