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

        List<Pair<Long, Long>> conditions = Lists.newArrayList();
        extractTsConditionInternal(rootFilter, tsColRef, conditions);

        if (conditions.size() > 0) {
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

    private static void extractTsConditionInternal(TupleFilter filter, TblColRef colRef, List<Pair<Long, Long>> conditions) {
        if (filter instanceof LogicalTupleFilter) {
            if (filter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
                for (TupleFilter child : filter.getChildren()) {
                    extractTsConditionInternal(child, colRef, conditions);
                }
            } else {
                return;
            }
        }

        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter compareTupleFilter = (CompareTupleFilter) filter;
            if (compareTupleFilter.getColumn() == null)// column will be null at filters like " 1<>1"
                return;

            if (compareTupleFilter.getColumn().equals(colRef)) {
                Object firstValue = compareTupleFilter.getFirstValue();
                long t;
                switch (compareTupleFilter.getOperator()) {
                case EQ:
                    t = DateFormat.stringToMillis((String) firstValue);
                    conditions.add(Pair.of(t, t));
                    break;
                case NEQ:
                    break;
                case LT:
                case LTE:
                    t = DateFormat.stringToMillis((String) firstValue);
                    conditions.add(Pair.<Long, Long> of(null, t));
                    break;
                case GT:
                case GTE:
                    t = DateFormat.stringToMillis((String) firstValue);
                    conditions.add(Pair.<Long, Long> of(t, null));
                    break;
                case IN://In is not handled for now
                    break;
                default:
                }
            }
        }
    }
}
