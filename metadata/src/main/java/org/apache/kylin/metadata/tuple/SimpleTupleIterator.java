package org.apache.kylin.metadata.tuple;

import java.util.Iterator;

import com.google.common.collect.Range;

/**
 *
 */
public class SimpleTupleIterator implements ITupleIterator {

    private Iterator<ITuple> backend;

    public SimpleTupleIterator(Iterator<ITuple> backend) {
        this.backend = backend;
    }

    @Override
    public boolean hasNext() {
        return backend.hasNext();
    }

    @Override
    public ITuple next() {
        return backend.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }

    @Override
    public Range<Long> getCacheExcludedPeriod() {
        return null;
    }
}
