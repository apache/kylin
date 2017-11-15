package org.apache.calcite.plan;

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

public class OLAPRelMdRowCount extends RelMdRowCount {
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
            .reflectiveSource(BuiltInMethod.ROW_COUNT.method, new OLAPRelMdRowCount());

    //avoid case like sql/query60.sql, which will generate plan:
    /*
    EnumerableLimit(fetch=[3])
        OLAPToEnumerableConverter
            OLAPAggregateRel(group=[{0}], SUM_PRICE=[SUM($1)], CNT_1=[COUNT()], TOTAL_ITEMS=[SUM($2)])
     */
    private boolean shouldIntercept(RelNode rel) {
        for (RelNode input : rel.getInputs()) {
            if (input instanceof RelSubset) {
                RelSubset relSubset = (RelSubset) input;
                if (relSubset.getBest() != null
                        && relSubset.getBest().getClass().getCanonicalName().endsWith("OLAPToEnumerableConverter")) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;
        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(RelSubset subset, RelMetadataQuery mq) {
        if (shouldIntercept(subset))
            return 1E10;

        return super.getRowCount(subset, mq);
    }

    @Override
    public Double getRowCount(Union rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(Intersect rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(Minus rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(Filter rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(Calc rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(Project rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(Sort rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(EnumerableLimit rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(SingleRel rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(Join rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(SemiJoin rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }

    @Override
    public Double getRowCount(Values rel, RelMetadataQuery mq) {
        if (shouldIntercept(rel))
            return 1E10;

        return super.getRowCount(rel, mq);
    }
}
