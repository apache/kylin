package org.apache.kylin.metadata.filter;

import com.google.common.collect.Maps;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.IdentityHashMap;

/**
 */
public class TsConditionEraser implements TupleFilterSerializer.Decorator {

    private final TblColRef tsColumn;
    private final TupleFilter root;

    private IdentityHashMap<TupleFilter, Boolean> isInTopLevelANDs;

    public TsConditionEraser(TblColRef tsColumn, TupleFilter root) {
        this.tsColumn = tsColumn;
        this.root = root;
        this.isInTopLevelANDs = Maps.newIdentityHashMap();
    }

    /**
     * replace filter on timestamp column to null, so that two tuple filter trees can
     * be compared regardless of the filter condition on timestamp column (In top level where conditions concatenated by ANDs)
     * @param filter
     * @return
     */
    @Override
    public TupleFilter onSerialize(TupleFilter filter) {

        if (filter == null)
            return null;

        //we just need reference equal
        if (root == filter) {
            isInTopLevelANDs.put(filter, true);
        }

        if (isInTopLevelANDs.containsKey(filter)) {
            classifyChildrenByMarking(filter);

            if (filter instanceof CompareTupleFilter) {
                TblColRef c = ((CompareTupleFilter) filter).getColumn();
                if (c != null && c.equals(tsColumn)) {
                    return null;
                }
            }
        }

        return filter;
    }

    private void classifyChildrenByMarking(TupleFilter filter) {
        if (filter instanceof LogicalTupleFilter) {
            if (filter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
                for (TupleFilter child : filter.getChildren()) {
                    isInTopLevelANDs.put(child, true);
                }
            }
        }
    }
}
