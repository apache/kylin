package org.apache.kylin.metadata.tuple;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

/**
 *
 * Like "tee" command in linux, it effectively duplicates the underlying
 * ITupleIterator's results
 */
public class TeeTupleIterator implements ITupleIterator {

    private Function<List<ITuple>, Void> actionOnSeeingWholeData;
    private ITupleIterator underlying;
    private List<ITuple> duplicatedData;

    public TeeTupleIterator(ITupleIterator underlying) {
        this.underlying = underlying;
        this.duplicatedData = Lists.newArrayList();
    }

    public void setActionOnSeeingWholeData(Function<List<ITuple>, Void> actionOnSeeingWholeData) {
        this.actionOnSeeingWholeData = actionOnSeeingWholeData;
    }

    @Override
    public void close() {
        this.underlying.close();
        //if(this.underlying.isDrained)
        actionOnSeeingWholeData.apply(duplicatedData);
    }

    @Override
    public Range<Long> getCacheExcludedPeriod() {
        return this.underlying.getCacheExcludedPeriod();
    }

    @Override
    public boolean hasNext() {
        return this.underlying.hasNext();
    }

    @Override
    public ITuple next() {
        ITuple ret = this.underlying.next();
        duplicatedData.add(ret.makeCopy());
        return ret;
    }

    @Override
    public void remove() {
        this.underlying.remove();
    }
}
