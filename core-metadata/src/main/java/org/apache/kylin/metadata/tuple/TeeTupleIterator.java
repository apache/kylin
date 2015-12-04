package org.apache.kylin.metadata.tuple;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * Like "tee" command in linux, it effectively duplicates the underlying
 * ITupleIterator's results
 */
public class TeeTupleIterator implements ITupleIterator {

    private ITupleIterator underlying;
    private List<ITuple> duplicatedData;
    private List<TeeTupleItrListener> listeners = Lists.newArrayList();
    private long createTime;

    public TeeTupleIterator(ITupleIterator underlying) {
        this.underlying = underlying;
        this.duplicatedData = Lists.newArrayList();
        this.createTime = System.currentTimeMillis();
    }

    @Override
    public void close() {
        this.underlying.close();

        for (TeeTupleItrListener listener : this.listeners) {
            listener.notify(this.duplicatedData, this.createTime);
        }
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

    public void addCloseListener(TeeTupleItrListener listener) {
        this.listeners.add(listener);
    }
}
