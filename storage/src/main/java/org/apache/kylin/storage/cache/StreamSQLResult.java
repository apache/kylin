package org.apache.kylin.storage.cache;

import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.metadata.tuple.ITuple;

import com.google.common.collect.Range;

/**
 * Created by Hongbin Ma(Binmahone) on 4/13/15.
 */
public class StreamSQLResult {
    private List<ITuple> rows;
    private Range<Long> timeCovered;

    public StreamSQLResult(List<ITuple> rows, Range<Long> timeCovered) {
        this.rows = rows;
        this.timeCovered = timeCovered;
    }

    public Range<Long> getReusableResults(Range<Long> tsRange) {
        if (tsRange.equals(timeCovered))
            return timeCovered;

        if (timeCovered.isConnected(tsRange)) {
            //share nothing in common
            return null;
        }

        Range<Long> ret = timeCovered.intersection(tsRange);
        return ret.isEmpty() ? null : ret;
    }

    public Iterator<ITuple> reuse(Range<Long> reusablePeriod) {
        //TODO: currently regardless of reusablePeriod, all rows are returned
        return rows.iterator();
    }

    @Override
    public String toString() {
        return rows.size() + " tuples cached for period " + RangeUtil.formatTsRange(timeCovered);
    }
}
