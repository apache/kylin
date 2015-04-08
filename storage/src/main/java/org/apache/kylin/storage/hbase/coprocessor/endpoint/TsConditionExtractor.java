package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.util.DateFormat;

import com.google.common.collect.Lists;

/**
 * Created by Hongbin Ma(Binmahone) on 4/8/15.
 */
public class TsConditionExtractor {

    public static Pair<Long, Long> extractTsCondition(TableRecordInfo recordInfo, List<TblColRef> columns, TupleFilter rootFilter) {
        int tsCol = recordInfo.getTimestampColumn();
        TblColRef tsColRef = columns.get(tsCol);

        TupleFilter filter = rootFilter;
        if (filter instanceof CompareTupleFilter) {
            //where ts > 2000-01-01
            return extractTsConditionInternal((CompareTupleFilter) filter, tsColRef);
        } else if (filter instanceof LogicalTupleFilter) {
            LogicalTupleFilter logicFilter = (LogicalTupleFilter) filter;
            if (logicFilter.getOperator() != TupleFilter.FilterOperatorEnum.AND) {
                return null;
            }
            //where ts > 2000-01-01 and ts < 2001-01-01 and xColumn > 1000
            List<Pair<Long, Long>> conditions = Lists.newArrayList();
            for (TupleFilter child : logicFilter.getChildren()) {
                if (child instanceof CompareTupleFilter) {
                    Pair<Long, Long> piece = extractTsConditionInternal((CompareTupleFilter) child, tsColRef);
                    if (piece != null) {
                        conditions.add(piece);
                    }
                }
            }

            long min = Long.MIN_VALUE;
            long max = Long.MAX_VALUE;
            for (Pair<Long, Long> pair : conditions) {
                if (pair.getLeft() != null && pair.getLeft() > min) {
                    min = pair.getLeft();
                }
                if (pair.getRight() != null && pair.getRight() < max) {
                    max = pair.getRight();
                }
            }
            return Pair.of(min == Long.MIN_VALUE ? null : min, max == Long.MAX_VALUE ? null : max);
        }
        return null;
    }

    private static Pair<Long, Long> extractTsConditionInternal(CompareTupleFilter filter, TblColRef colRef) {
        if (filter.getColumn().equals(colRef)) {
            Object firstValue = filter.getFirstValue();
            long t;
            switch (filter.getOperator()) {
            case EQ:
                t = DateFormat.stringToMillis((String) firstValue);
                return Pair.of(t, t);
            case NEQ:
                return null;
            case LT:
            case LTE:
                t = DateFormat.stringToMillis((String) firstValue);
                return Pair.of(null, t);
            case GT:
            case GTE:
                t = DateFormat.stringToMillis((String) firstValue);
                return Pair.of(t, null);
            case IN://In is not handled for now
                return null;
            default:
                return null;
            }
        } else {
            return null;
        }
    }

}
