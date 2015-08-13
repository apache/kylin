package org.apache.kylin.metadata.tuple;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

/**
 */
public class CompoundTupleIterator implements ITupleIterator {
    private static final Logger logger = LoggerFactory.getLogger(CompoundTupleIterator.class);
    private List<ITupleIterator> backends;
    private Iterator<ITuple> compoundIterator;

    public CompoundTupleIterator(List<ITupleIterator> backends) {
        Preconditions.checkArgument(backends != null && backends.size() != 0, "backends not exists");
        this.backends = backends;
        this.compoundIterator = Iterators.concat(backends.iterator());
    }

    @Override
    public void close() {
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
