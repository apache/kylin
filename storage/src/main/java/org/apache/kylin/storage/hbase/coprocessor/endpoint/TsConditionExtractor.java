package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

/**
 */
public class TsConditionExtractor {

    /**
     *
     * @return null if the ts range conflicts with each other
     *         Ranges.all() if no ts condition is defined(in surfaced ANDs)
     */
    public static Range<Long> extractTsCondition(TblColRef tsColRef, TupleFilter rootFilter) {
        return extractTsConditionInternal(rootFilter, tsColRef);
    }

    private static Range<Long> extractTsConditionInternal(TupleFilter filter, TblColRef colRef) {
        if (filter == null) {
            return Ranges.all();
        }

        if (filter instanceof LogicalTupleFilter) {
            if (filter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
                Range<Long> ret = Ranges.all();
                for (TupleFilter child : filter.getChildren()) {
                    Range childRange = extractTsConditionInternal(child, colRef);
                    if (childRange != null) {
                        if (ret.isConnected(childRange) && !ret.intersection(childRange).isEmpty()) {
                            ret = ret.intersection(childRange);
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                }
                return ret.isEmpty() ? null : ret;
            } else {
                //for conditions like date > DATE'2000-11-11' OR date < DATE '1999-01-01'
                //we will use Ranges.all() rather than two ranges to represent them
                return Ranges.all();
            }
        }

        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter compareTupleFilter = (CompareTupleFilter) filter;
            if (compareTupleFilter.getColumn() == null)// column will be null at filters like " 1<>1"
                return Ranges.all();

            if (compareTupleFilter.getColumn().equals(colRef)) {
                Object firstValue = compareTupleFilter.getFirstValue();
                long t;
                switch (compareTupleFilter.getOperator()) {
                case EQ:
                    t = DateFormat.stringToMillis((String) firstValue);
                    return Ranges.closed(t, t);
                case LT:
                    t = DateFormat.stringToMillis((String) firstValue);
                    return Ranges.lessThan(t);
                case LTE:
                    t = DateFormat.stringToMillis((String) firstValue);
                    return Ranges.atMost(t);
                case GT:
                    t = DateFormat.stringToMillis((String) firstValue);
                    return Ranges.greaterThan(t);
                case GTE:
                    t = DateFormat.stringToMillis((String) firstValue);
                    return Ranges.atLeast(t);
                case NEQ:
                case IN://not handled for now
                    break;
                default:
                }
            }
        }
        return Ranges.all();
    }
}
