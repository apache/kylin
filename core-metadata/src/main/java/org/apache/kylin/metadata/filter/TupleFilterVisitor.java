package org.apache.kylin.metadata.filter;

public interface TupleFilterVisitor<R> {

    R visitCase(CaseTupleFilter filter);

    R visitColumn(ColumnTupleFilter filter);

    R visitCompare(CompareTupleFilter filter);

    R visitConstant(ConstantTupleFilter filter);

    R visitDynamic(DynamicTupleFilter filter);

    R visitFunction(FunctionTupleFilter filter);

    R visitLogical(LogicalTupleFilter filter);

    R visitExtract(ExtractTupleFilter filter);

    R visitUnsupported(UnsupportedTupleFilter filter);
}
