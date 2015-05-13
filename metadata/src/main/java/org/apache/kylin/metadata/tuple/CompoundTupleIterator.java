package org.apache.kylin.metadata.tuple;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public Range<Long> getCacheExcludedPeriod() {
        Range<Long> timeSpan = null;
        for (ITupleIterator itt : this.backends) {
            Range<Long> excluded = itt.getCacheExcludedPeriod();
            if (excluded != null && !excluded.isEmpty()) {
                if (timeSpan == null) {
                    //first one
                    timeSpan = excluded;
                } else {
                    logger.warn("There are two excluded period, use a span to replace them: " + timeSpan + " and " + excluded );
                    timeSpan = timeSpan.span(excluded);
                }
            }
        }
        return timeSpan;
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
