package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;

public class GTUtil {

    static final TableDesc MOCKUP_TABLE = TableDesc.mockup("GT_MOCKUP_TABLE");

    static TblColRef tblColRef(int col, String datatype) {
        ColumnDesc desc = ColumnDesc.mockup(MOCKUP_TABLE, col + 1, "" + col, datatype);
        return new TblColRef(desc);
    }

    public static TupleFilter convertFilterUnevaluatable(TupleFilter rootFilter, GTInfo info, //
            Set<TblColRef> unevaluatableColumnCollector) {
        return convertFilter(rootFilter, info, null, false, unevaluatableColumnCollector);
    }

    public static TupleFilter convertFilterConstants(TupleFilter rootFilter, GTInfo info) {
        return convertFilter(rootFilter, info, null, true, null);
    }

    public static TupleFilter convertFilterColumnsAndConstants(TupleFilter rootFilter, GTInfo info, //
            List<TblColRef> colMapping, Set<TblColRef> unevaluatableColumnCollector) {
        return convertFilter(rootFilter, info, colMapping, true, unevaluatableColumnCollector);
    }

    // converts TblColRef to GridTable column, encode constants, drop unEvaluatable parts
    private static TupleFilter convertFilter(TupleFilter rootFilter, final GTInfo info, //
            final List<TblColRef> colMapping, final boolean encodeConstants, //
            final Set<TblColRef> unevaluatableColumnCollector) {

        byte[] bytes = TupleFilterSerializer.serialize(rootFilter, new TupleFilterSerializer.Decorator() {
            @Override
            public TupleFilter onSerialize(TupleFilter filter) {
                if (filter == null)
                    return null;

                // In case of NOT(unEvaluatableFilter), we should immediately replace it as TRUE,
                // Otherwise, unEvaluatableFilter will later be replace with TRUE and NOT(unEvaluatableFilter)
                // will always return FALSE.
                if (filter.getOperator() == TupleFilter.FilterOperatorEnum.NOT && !TupleFilter.isEvaluableRecursively(filter)) {
                    TupleFilter.collectColumns(filter, unevaluatableColumnCollector);
                    return ConstantTupleFilter.TRUE;
                }

                // shortcut for unEvaluatable filter
                if (filter.isEvaluable() == false) {
                    TupleFilter.collectColumns(filter, unevaluatableColumnCollector);
                    return ConstantTupleFilter.TRUE;
                }

                // map to column onto grid table
                if (colMapping != null && filter instanceof ColumnTupleFilter) {
                    ColumnTupleFilter colFilter = (ColumnTupleFilter) filter;
                    int gtColIdx = colMapping.indexOf(colFilter.getColumn());
                    return new ColumnTupleFilter(info.colRef(gtColIdx));
                }

                // encode constants
                if (encodeConstants && filter instanceof CompareTupleFilter) {
                    return encodeConstants((CompareTupleFilter) filter);
                }

                return filter;
            }

            @SuppressWarnings({ "rawtypes", "unchecked" })
            private TupleFilter encodeConstants(CompareTupleFilter oldCompareFilter) {
                // extract ColumnFilter & ConstantFilter
                TblColRef externalCol = oldCompareFilter.getColumn();

                if (externalCol == null) {
                    return oldCompareFilter;
                }

                Collection constValues = oldCompareFilter.getValues();
                if (constValues == null || constValues.isEmpty()) {
                    return oldCompareFilter;
                }

                CompareTupleFilter newCompareFilter = new CompareTupleFilter(oldCompareFilter.getOperator());
                newCompareFilter.addChild(new ColumnTupleFilter(externalCol));

                Object firstValue = constValues.iterator().next();
                int col = colMapping == null ? externalCol.getColumn().getZeroBasedIndex() : colMapping.indexOf(externalCol);

                TupleFilter result;
                ByteArray code;

                // translate constant into code
                switch (newCompareFilter.getOperator()) {
                case EQ:
                case IN:
                    Set newValues = Sets.newHashSet();
                    for (Object value : constValues) {
                        code = translate(col, value, 0);
                        if (code != null)
                            newValues.add(code);
                    }
                    if (newValues.isEmpty()) {
                        result = ConstantTupleFilter.FALSE;
                    } else {
                        newCompareFilter.addChild(new ConstantTupleFilter(newValues));
                        result = newCompareFilter;
                    }
                    break;
                case NEQ:
                    code = translate(col, firstValue, 0);
                    if (code == null) {
                        result = ConstantTupleFilter.TRUE;
                    } else {
                        newCompareFilter.addChild(new ConstantTupleFilter(code));
                        result = newCompareFilter;
                    }
                    break;
                case LT:
                    code = translate(col, firstValue, 1);
                    if (code == null) {
                        result = ConstantTupleFilter.TRUE;
                    } else {
                        newCompareFilter.addChild(new ConstantTupleFilter(code));
                        result = newCompareFilter;
                    }
                    break;
                case LTE:
                    code = translate(col, firstValue, -1);
                    if (code == null) {
                        result = ConstantTupleFilter.FALSE;
                    } else {
                        newCompareFilter.addChild(new ConstantTupleFilter(code));
                        result = newCompareFilter;
                    }
                    break;
                case GT:
                    code = translate(col, firstValue, -1);
                    if (code == null) {
                        result = ConstantTupleFilter.TRUE;
                    } else {
                        newCompareFilter.addChild(new ConstantTupleFilter(code));
                        result = newCompareFilter;
                    }
                    break;
                case GTE:
                    code = translate(col, firstValue, 1);
                    if (code == null) {
                        result = ConstantTupleFilter.FALSE;
                    } else {
                        newCompareFilter.addChild(new ConstantTupleFilter(code));
                        result = newCompareFilter;
                    }
                    break;
                default:
                    throw new IllegalStateException("Cannot handle operator " + newCompareFilter.getOperator());
                }
                return result;
            }

            transient ByteBuffer buf = ByteBuffer.allocate(info.getMaxColumnLength());

            private ByteArray translate(int col, Object value, int roundingFlag) {
                try {
                    buf.clear();
                    info.codeSystem.encodeColumnValue(col, value, roundingFlag, buf);
                    return ByteArray.copyOf(buf.array(), 0, buf.position());
                } catch (IllegalArgumentException ex) {
                    return null;
                }
            }
        }, info.codeSystem.getFilterCodeSystem());

        return TupleFilterSerializer.deserialize(bytes, info.codeSystem.getFilterCodeSystem());
    }

}
