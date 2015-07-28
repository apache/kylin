package org.apache.kylin.gridtable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;

public class GTScanRequest {

    private GTInfo info;
    private GTScanRange range;
    private ImmutableBitSet columns;
    private ImmutableBitSet selectedColBlocks;

    // optional filtering
    private TupleFilter filterPushDown;

    // optional aggregation
    private ImmutableBitSet aggrGroupBy;
    private ImmutableBitSet aggrMetrics;
    private String[] aggrMetricsFuncs;

    // hint to storage behavior
    private boolean allowPreAggregation = true;

    public GTScanRequest(GTInfo info) {
        this(info, null, null, null);
    }

    public GTScanRequest(GTInfo info, GTScanRange range, ImmutableBitSet columns, TupleFilter filterPushDown) {
        this.info = info;
        this.range = range == null ? new GTScanRange(new GTRecord(info), new GTRecord(info)) : range;
        this.columns = columns;
        this.filterPushDown = filterPushDown;
        validate(info);
    }

    public GTScanRequest(GTInfo info, GTScanRange range, ImmutableBitSet aggrGroupBy, ImmutableBitSet aggrMetrics, //
            String[] aggrMetricsFuncs, TupleFilter filterPushDown) {
        this(info, range, null, aggrGroupBy, aggrMetrics, aggrMetricsFuncs, filterPushDown, true);
    }

    public GTScanRequest(GTInfo info, GTScanRange range, ImmutableBitSet dimensions, ImmutableBitSet aggrGroupBy, //
            ImmutableBitSet aggrMetrics, String[] aggrMetricsFuncs, TupleFilter filterPushDown, boolean allowPreAggregation) {
        this.info = info;
        this.range = range == null ? new GTScanRange(new GTRecord(info), new GTRecord(info)) : range;
        this.columns = dimensions;
        this.filterPushDown = filterPushDown;

        this.aggrGroupBy = aggrGroupBy;
        this.aggrMetrics = aggrMetrics;
        this.aggrMetricsFuncs = aggrMetricsFuncs;

        this.allowPreAggregation = allowPreAggregation;

        validate(info);
    }

    private void validate(GTInfo info) {
        if (range == null)
            range = new GTScanRange(null, null);

        if (hasAggregation()) {
            if (aggrGroupBy.intersects(aggrMetrics))
                throw new IllegalStateException();
            if (aggrMetrics.cardinality() != aggrMetricsFuncs.length)
                throw new IllegalStateException();

            if (columns == null)
                columns = ImmutableBitSet.EMPTY;
            columns = columns.or(aggrGroupBy);
            columns = columns.or(aggrMetrics);
        }

        if (columns == null)
            columns = info.colAll;

        this.selectedColBlocks = info.selectColumnBlocks(columns);

        if (hasFilterPushDown()) {
            validateFilterPushDown(info);
        }
    }

    private void validateFilterPushDown(GTInfo info) {
        if (hasFilterPushDown() == false)
            return;

        Set<TblColRef> filterColumns = Sets.newHashSet();
        TupleFilter.collectColumns(filterPushDown, filterColumns);

        for (TblColRef col : filterColumns) {
            // filter columns must belong to the table
            info.validateColRef(col);
            // filter columns must be returned to satisfy upper layer evaluation (calcite)
            columns = columns.set(col.getColumnDesc().getZeroBasedIndex());
        }

        // un-evaluatable filter must be removed
        if (TupleFilter.isEvaluableRecursively(filterPushDown) == false) {
            Set<TblColRef> unevaluableColumns = Sets.newHashSet();
            filterPushDown = GTUtil.convertFilterUnevaluatable(filterPushDown, info, unevaluableColumns);

            // columns in un-evaluatable filter must be returned without loss so upper layer can do final evaluation
            if (hasAggregation()) {
                for (TblColRef col : unevaluableColumns) {
                    aggrGroupBy = aggrGroupBy.set(col.getColumnDesc().getZeroBasedIndex());
                }
            }
        }
    }

    public IGTScanner decorateScanner(IGTScanner scanner) throws IOException {
        IGTScanner result = scanner;
        if (this.hasFilterPushDown()) {
            result = new GTFilterScanner(result, this);
        }
        if (this.allowPreAggregation && this.hasAggregation()) {
            result = new GTAggregateScanner(result, this);
        }
        return result;
    }

    public boolean hasFilterPushDown() {
        return filterPushDown != null;
    }

    //TODO BUG?  select sum() from fact, no aggr by
    public boolean hasAggregation() {
        return aggrGroupBy != null && aggrMetrics != null && aggrMetricsFuncs != null;
    }

    public GTInfo getInfo() {
        return info;
    }

    public GTRecord getPkStart() {
        return range.pkStart;
    }

    public GTRecord getPkEnd() {
        return range.pkEnd;
    }

    public ImmutableBitSet getSelectedColBlocks() {
        return selectedColBlocks;
    }

    public ImmutableBitSet getColumns() {
        return columns;
    }

    public TupleFilter getFilterPushDown() {
        return filterPushDown;
    }

    public ImmutableBitSet getAggrGroupBy() {
        return aggrGroupBy;
    }

    public ImmutableBitSet getAggrMetrics() {
        return aggrMetrics;
    }

    public String[] getAggrMetricsFuncs() {
        return aggrMetricsFuncs;
    }

    @Override
    public String toString() {
        return "GTScanRequest [range=" + range + ", columns=" + columns + ", filterPushDown=" + filterPushDown + ", aggrGroupBy=" + aggrGroupBy + ", aggrMetrics=" + aggrMetrics + ", aggrMetricsFuncs=" + Arrays.toString(aggrMetricsFuncs) + "]";
    }

}
