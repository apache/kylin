package org.apache.kylin.metadata.tuple;

import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 4/14/15.
 */
public class CompoundTupleIterator implements ITupleIterator {
    private List<ITupleIterator> backends;
    private Iterator<ITuple> compoundIterator;

    public CompoundTupleIterator(List<ITupleIterator> backends) {
        this.backends = backends;
        this.compoundIterator = Iterators.concat(backends.iterator());
    }

    @Override
    public void close() throws IOException {
        for (ITupleIterator i : backends) {
            i.close();
        }
    }

    @Override
    public boolean hasNext() {
        return this.compoundIterator.hasNext();
    }

    @Override
    public ITuple next() {
        return this.compoundIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
