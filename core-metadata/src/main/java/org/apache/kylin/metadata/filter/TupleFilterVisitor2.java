package org.apache.kylin.metadata.filter;

import org.apache.kylin.metadata.model.TblColRef;

import java.util.List;
import java.util.Set;

/**
 * A simplified version of {@link TupleFilterVisitor}.
 * @param <R>
 */
public interface TupleFilterVisitor2<R> {

    R visitColumnCompare(CompareTupleFilter originFilter, TblColRef column, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue);

    R visitCaseCompare(CompareTupleFilter originFilter, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue, TupleFilterVisitor2Adaptor<R> adaptor);

    R visitColumnLike(BuiltInFunctionTupleFilter originFilter, TblColRef column, String pattern, boolean reversed);

    R visitColumnFunction(CompareTupleFilter originFilter, BuiltInFunctionTupleFilter function, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue);

    R visitAnd(LogicalTupleFilter originFilter, List<? extends TupleFilter> children, TupleFilterVisitor2Adaptor<R> adaptor);

    R visitOr(LogicalTupleFilter originFilter, List<? extends TupleFilter> children, TupleFilterVisitor2Adaptor<R> adaptor);

    R visitNot(LogicalTupleFilter originFilter, TupleFilter child, TupleFilterVisitor2Adaptor<R> adaptor);

    R visitCase(CaseTupleFilter originFilter, List<? extends TupleFilter> children, List<? extends TupleFilter> whenFilter, List<? extends TupleFilter> thenFilte, TupleFilterVisitor2Adaptor<R> adaptor);

    R visitColumn(ColumnTupleFilter originFilter, TblColRef column);

    R visitSecondColumnCompare(CompareTupleFilter originFilter);

    /**
     * @param originFilter ConstantTupleFilter.TRUE or ConstantTupleFilter.FALSE
     * @return
     */
    R visitConstant(ConstantTupleFilter originFilter);

    R visitUnsupported(TupleFilter originFilter);
}

