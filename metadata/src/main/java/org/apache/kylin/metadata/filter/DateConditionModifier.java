package org.apache.kylin.metadata.filter;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Created by Hongbin Ma(Binmahone) on 4/13/15.
 */
public class DateConditionModifier implements TupleFilterSerializer.Decorator {

    private IdentityHashMap<TupleFilter, Boolean> dateCompareTupleChildren;

    public DateConditionModifier(TupleFilter root) {
        this.dateCompareTupleChildren = Maps.newIdentityHashMap();
    }

    /**
     * replace filter on timestamp column to null, so that two tuple filter trees can
     * be compared regardless of the filter condition on timestamp column (In top level where conditions concatenated by ANDs)
     * @param filter
     * @return
     */
    @Override
    public TupleFilter onSerialize(TupleFilter filter) {

        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter cfilter = (CompareTupleFilter) filter;
            List<? extends TupleFilter> children = cfilter.getChildren();

            if (children == null || children.size() < 1) {
                throw new IllegalArgumentException("Illegal compare filter: " + cfilter);
            }

            TblColRef col = cfilter.getColumn();
            if (col == null || (!"date".equals(col.getDatatype()))) {
                return cfilter;
            }

            for (TupleFilter child : filter.getChildren()) {
                dateCompareTupleChildren.put(child, true);
            }
        }

        if (filter instanceof ConstantTupleFilter && dateCompareTupleChildren.containsKey(filter)) {
            ConstantTupleFilter constantTupleFilter = (ConstantTupleFilter) filter;
            Set<String> newValues = Sets.newHashSet();

            for (String value : (Collection<String>) constantTupleFilter.getValues()) {
                newValues.add(DateFormat.formatToDateStr(Long.valueOf(value)));
            }
            return new ConstantTupleFilter(newValues);
        }
        return filter;
    }
}
