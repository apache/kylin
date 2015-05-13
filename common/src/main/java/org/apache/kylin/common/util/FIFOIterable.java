package org.apache.kylin.common.util;

import java.util.Iterator;
import java.util.Queue;

/**
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
