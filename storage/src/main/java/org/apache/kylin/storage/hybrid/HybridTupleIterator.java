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
        if (iterators[currentIndex].hasNext())
            return true;

        while (!iterators[currentIndex].hasNext() && currentIndex + 1 < iterators.length) {
            currentIndex++;
        }

        return iterators[currentIndex].hasNext();
    }

    @Override
    public ITuple next() {
        while (!iterators[currentIndex].hasNext() && currentIndex + 1 < iterators.length) {
            currentIndex++;
        }

        return iterators[currentIndex].next();
    }

    @Override
    public void close() {
        for (ITupleIterator i : iterators) {
            i.close();
        }
    }
}
