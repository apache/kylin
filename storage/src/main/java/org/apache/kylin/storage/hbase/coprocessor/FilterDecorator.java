package org.apache.kylin.storage.hbase.coprocessor;

import com.google.common.collect.Sets;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.IDictionaryAware;
import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.Collection;
import java.util.Set;

/**
 * Created by Hongbin Ma(Binmahone) on 3/3/15.
 */
@SuppressWarnings("unchecked")
public class FilterDecorator implements TupleFilterSerializer.Decorator {
    public enum FilterConstantsTreatment {
        AS_IT_IS, REPLACE_WITH_GLOBAL_DICT, REPLACE_WITH_LOCAL_DICT
    }

    private RowKeyColumnIO columnIO;
    private Set<TblColRef> unstrictlyFilteredColumns;
    private FilterConstantsTreatment filterConstantsTreatment;

    public FilterDecorator(IDictionaryAware seg, FilterConstantsTreatment filterConstantsTreatment) {
        this.columnIO = new RowKeyColumnIO(seg);
        this.unstrictlyFilteredColumns = Sets.newHashSet();
        this.filterConstantsTreatment = filterConstantsTreatment;
    }

    public Set<TblColRef> getUnstrictlyFilteredColumns() {
        return unstrictlyFilteredColumns;
    }


    private TupleFilter replaceConstantsWithLocalDict(CompareTupleFilter oldCompareFilter, CompareTupleFilter newCompareFilter) {
        //TODO localdict: (performance issue) transalte() with roundingflag 0 will use try catch exceptions to deal with non-existing entries
        return replaceConstantsWithGlobalDict(oldCompareFilter,newCompareFilter);
    }

    private TupleFilter replaceConstantsWithGlobalDict(CompareTupleFilter oldCompareFilter, CompareTupleFilter newCompareFilter) {
        Collection<String> constValues = (Collection<String>) oldCompareFilter.getValues();
        String firstValue = constValues.iterator().next();
        TblColRef col = newCompareFilter.getColumn();

        TupleFilter result;
        String v;

        // translate constant into rowkey ID
        switch (newCompareFilter.getOperator()) {
        case EQ:
        case IN:
            Set<String> newValues = Sets.newHashSet();
            for (String value : constValues) {
                v = translate(col, value, 0);
                if (!isDictNull(v))
                    newValues.add(v);
            }
            if (newValues.isEmpty()) {
                result = ConstantTupleFilter.FALSE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(newValues));
                result = newCompareFilter;
            }
            break;
        case NEQ:
            v = translate(col, firstValue, 0);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.TRUE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        case LT:
            v = translate(col, firstValue, 1);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.TRUE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        case LTE:
            v = translate(col, firstValue, -1);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.FALSE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        case GT:
            v = translate(col, firstValue, -1);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.TRUE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        case GTE:
            v = translate(col, firstValue, 1);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.FALSE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        default:
            throw new IllegalStateException("Cannot handle operator " + newCompareFilter.getOperator());
        }
        return result;
    }

    private boolean isDictNull(String v) {
        return DictCodeSystem.INSTANCE.isNull(v);
    }

    @Override
    public TupleFilter onSerialize(TupleFilter filter) {
        if (filter == null)
            return null;

        // In case of NOT(unEvaluatableFilter), we should immediately replace it as TRUE,
        // Otherwise, unEvaluatableFilter will later be replace with TRUE and NOT(unEvaluatableFilter) will
        // always return FALSE
        if (filter.getOperator() == TupleFilter.FilterOperatorEnum.NOT && !TupleFilter.isEvaluableRecursively(filter)) {
            TupleFilter.collectColumns(filter, unstrictlyFilteredColumns);
            return ConstantTupleFilter.TRUE;
        }

        if (!(filter instanceof CompareTupleFilter))
            return filter;

        if (!TupleFilter.isEvaluableRecursively(filter)) {
            TupleFilter.collectColumns(filter, unstrictlyFilteredColumns);
            return ConstantTupleFilter.TRUE;
        }

        if (filterConstantsTreatment == FilterConstantsTreatment.AS_IT_IS) {
            return filter;
        } else {

            // extract ColumnFilter & ConstantFilter
            CompareTupleFilter compareFilter = (CompareTupleFilter) filter;
            TblColRef col = compareFilter.getColumn();

            if (col == null) {
                return filter;
            }

            Collection<String> constValues = (Collection<String>) compareFilter.getValues();
            if (constValues == null || constValues.isEmpty()) {
                return filter;
            }

            CompareTupleFilter newCompareFilter = new CompareTupleFilter(compareFilter.getOperator());
            newCompareFilter.addChild(new ColumnTupleFilter(col));

            if (filterConstantsTreatment == FilterConstantsTreatment.REPLACE_WITH_GLOBAL_DICT) {
                return replaceConstantsWithGlobalDict(compareFilter, newCompareFilter);
            } else if (filterConstantsTreatment == FilterConstantsTreatment.REPLACE_WITH_LOCAL_DICT) {
                return replaceConstantsWithLocalDict(compareFilter, newCompareFilter);
            } else {
                throw new RuntimeException("should not reach here");
            }
        }
    }

    private String translate(TblColRef column, String v, int roundingFlag) {
        byte[] value = Bytes.toBytes(v);
        byte[] id = new byte[columnIO.getColumnLength(column)];
        columnIO.writeColumn(column, value, value.length, roundingFlag, Dictionary.NULL, id, 0);
        return Dictionary.dictIdToString(id, 0, id.length);
    }
}