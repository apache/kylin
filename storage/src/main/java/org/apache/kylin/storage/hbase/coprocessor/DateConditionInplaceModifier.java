package org.apache.kylin.storage.hbase.coprocessor;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;

import java.util.Collection;
import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 5/7/15.
 *
 * For historical reasons, date types are encoded with DateStrDcitionary
 * rather than TimeStrDictionary, so the constant in CompareTupleFilter should
 * be corrected to "yyyy-MM-dd" format for date types
 */
public class DateConditionInplaceModifier {

    public static void modify(TupleFilter filter) {

        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter cfilter = (CompareTupleFilter) filter;
            List<? extends TupleFilter> children = cfilter.getChildren();

            if (children == null || children.size() < 1) {
                throw new IllegalArgumentException("Illegal compare filter: " + cfilter);
            }

            if (cfilter.getColumn() == null || (!"date".equals(cfilter.getColumn().getColumnDesc().getTypeName()))) {
                return;
            }

            int nonConstantChild = 0;
            boolean firstUpdate = true;
            for (int i = 0; i < children.size(); ++i) {
                if (children.get(i) instanceof ConstantTupleFilter) {
                    ConstantTupleFilter constantAsFilter = (ConstantTupleFilter) children.get(i);
                    Collection<?> values = constantAsFilter.getValues();
                    Collection<Object> newValues = Lists.newArrayList();
                    for (Object x : values) {
                        newValues.add(x == null ? null : DateFormat.formatToDateStr(Long.parseLong((String) x)));
                    }
                    cfilter.updateValues(newValues, firstUpdate);
                    firstUpdate=false;
                } else {
                    nonConstantChild++;
                }
            }
            if (nonConstantChild != 1) {
                throw new IllegalArgumentException("Illegal compare filter: " + cfilter);
            }

            return;

        } else {
            for (TupleFilter child : filter.getChildren()) {
                modify(child);
            }
        }
    }
}
