package org.apache.kylin.metadata.tuple;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Hongbin Ma(Binmahone) on 4/15/15.
 *
 * Like "tee" command in linux,it effectively duplicates the underlying
 * ITupleIterator's results
 */
public class TeeTupleIterator implements ITupleIterator {

    private static final Logger logger = LoggerFactory.getLogger(TeeTupleIterator.class);

    private Function<List<ITuple>, Void> actionOnSeeingWholeData;
    private ITupleIterator underlying;
    private List<ITuple> duplicatedData;

    public TeeTupleIterator(ITupleIterator underlying, Function<List<ITuple>, Void> actionOnSeeingWholeData) {
        this.actionOnSeeingWholeData = actionOnSeeingWholeData;
        this.underlying = underlying;
        this.duplicatedData = Lists.newArrayList();
    }

    @Override
    public void close() {
        this.underlying.close();
        //if(this.underlying.isDrained)
        actionOnSeeingWholeData.apply(duplicatedData);
    }

    @Override
    public boolean hasNext() {
        return this.underlying.hasNext();
    }

    @Override
    public ITuple next() {
        ITuple ret = this.underlying.next();
        duplicatedData.add(ret);
        return ret;
    }

    @Override
    public void remove() {
        this.underlying.remove();
    }
}
