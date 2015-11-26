package org.apache.kylin.metadata.filter;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TimeConditionLiteralsReplacer implements TupleFilterSerializer.Decorator {

    private IdentityHashMap<TupleFilter, DataType> dateCompareTupleChildren;

    public TimeConditionLiteralsReplacer(TupleFilter root) {
        this.dateCompareTupleChildren = Maps.newIdentityHashMap();
    }

    @Override
    public TupleFilter onSerialize(TupleFilter filter) {

        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter cfilter = (CompareTupleFilter) filter;
            List<? extends TupleFilter> children = cfilter.getChildren();

            if (children == null || children.size() < 1) {
                throw new IllegalArgumentException("Illegal compare filter: " + cfilter);
            }

            TblColRef col = cfilter.getColumn();
            if (col == null || !col.getType().isDateTimeFamily()) {
                return cfilter;
            }

            for (TupleFilter child : filter.getChildren()) {
                dateCompareTupleChildren.put(child, col.getType());
            }
        }

        if (filter instanceof ConstantTupleFilter && dateCompareTupleChildren.containsKey(filter)) {
            ConstantTupleFilter constantTupleFilter = (ConstantTupleFilter) filter;
            Set<String> newValues = Sets.newHashSet();
            DataType columnType = dateCompareTupleChildren.get(filter);

            for (String value : (Collection<String>) constantTupleFilter.getValues()) {
                newValues.add(formatTime(Long.valueOf(value), columnType));
            }
            return new ConstantTupleFilter(newValues);
        }
        return filter;
    }

    private String formatTime(long millis, DataType dataType) {
        if (dataType.isDatetime() || dataType.isTime()) {
            throw new RuntimeException("Datetime and time type are not supported yet");
        }

        if (dataType.isTimestamp()) {
            return DateFormat.formatToTimeStr(millis);
        } else if (dataType.isDate()) {
            return DateFormat.formatToDateStr(millis);
        } else {
            throw new RuntimeException("Unknown type " + dataType + " to formatTime");
        }
    }
}
