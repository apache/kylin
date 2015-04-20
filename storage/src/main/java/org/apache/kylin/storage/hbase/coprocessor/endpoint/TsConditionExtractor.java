package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import com.google.common.collect.Ranges;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.common.util.DateFormat;

import com.google.common.collect.Range;

/**
 * Created by Hongbin Ma(Binmahone) on 4/8/15.
 */
public class TsConditionExtractor {

    public static Range<Long> extractTsCondition(TblColRef tsColRef, TupleFilter rootFilter) {
        return extractTsConditionInternal(rootFilter, tsColRef);
    }

    private static Range<Long> extractTsConditionInternal(TupleFilter filter, TblColRef colRef) {

        if (filter instanceof LogicalTupleFilter) {
            if (filter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
                Range ret = Ranges.all();
                for (TupleFilter child : filter.getChildren()) {
                    Range childRange = extractTsConditionInternal(child, colRef);
                    if (childRange != null) {
                        if (ret.isConnected(childRange) && !ret.intersection(childRange).isEmpty()) {
                            ret = ret.intersection(childRange);
                        } else {
                            return null;
                        }
                    }
                }
                return ret;
            } else {
                return null;
            }
        }

        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter compareTupleFilter = (CompareTupleFilter) filter;
            if (compareTupleFilter.getColumn() == null)// column will be null at filters like " 1<>1"
                return null;

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
        return null;
    }
}
