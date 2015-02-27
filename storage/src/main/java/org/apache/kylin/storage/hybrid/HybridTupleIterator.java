package org.apache.kylin.storage.hybrid;

import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;

/**
 * Created by shaoshi on 2/27/15.
 */
public class HybridTupleIterator implements ITupleIterator {

    private ITupleIterator[] iterators;

    private int currentIndex;

    public HybridTupleIterator(ITupleIterator[] iterators) {
        this.iterators = iterators;
        currentIndex = 0;
    }

    @Override
    public boolean hasNext() {
        return iterators[currentIndex].hasNext() || (currentIndex + 1 < iterators.length && iterators[currentIndex + 1].hasNext());
    }

    @Override
    public ITuple next() {
        if (!iterators[currentIndex].hasNext() && currentIndex + 1 < iterators.length) {
            currentIndex++;
        }

        return iterators[currentIndex].next();
    }

    @Override
    public void close() {

    }
}
