package org.apache.kylin.storage.gridtable;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;

import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;

public class GTScanRequest {

    // basic
    private GTInfo info;
    private GTScanRange range;
    private BitSet columns;

    // optional filtering
    private TupleFilter filterPushDown;

    // optional aggregation
    private BitSet aggrGroupBy;
    private BitSet aggrMetrics;
    private String[] aggrMetricsFuncs;

    public GTScanRequest(GTInfo info) {
        this(info, null, null, null);
    }

    public GTScanRequest(GTInfo info, GTScanRange range, BitSet columns, TupleFilter filterPushDown) {
        this.info = info;
        this.range = range == null ? new GTScanRange(new GTRecord(info), new GTRecord(info)) : range;
        this.columns = columns;
        this.filterPushDown = filterPushDown;
        validate();
    }

    public GTScanRequest(GTInfo info, GTScanRange range, BitSet aggrGroupBy, BitSet aggrMetrics, //
            String[] aggrMetricsFuncs, TupleFilter filterPushDown) {
        this(info, range, null, aggrGroupBy, aggrMetrics, aggrMetricsFuncs, filterPushDown);
    }

    public GTScanRequest(GTInfo info, GTScanRange range, BitSet dimensions, BitSet aggrGroupBy, BitSet aggrMetrics, //
            String[] aggrMetricsFuncs, TupleFilter filterPushDown) {
        this.info = info;
        this.range = range == null ? new GTScanRange(new GTRecord(info), new GTRecord(info)) : range;
        this.columns = dimensions;
        this.filterPushDown = filterPushDown;

        this.aggrGroupBy = aggrGroupBy;
        this.aggrMetrics = aggrMetrics;
        this.aggrMetricsFuncs = aggrMetricsFuncs;

        validate();
    }

    private void validate() {
        if (range == null)
            range = new GTScanRange(null, null);

        if (hasAggregation()) {
            if (aggrGroupBy.intersects(aggrMetrics))
                throw new IllegalStateException();
            if (aggrMetrics.cardinality() != aggrMetricsFuncs.length)
                throw new IllegalStateException();

            if (columns == null) {
                columns = new BitSet();
            }
            columns.or(aggrGroupBy);
            columns.or(aggrMetrics);
        }

        if (columns == null)
            columns = (BitSet) info.colAll.clone();

        if (hasFilterPushDown()) {
            validateFilterPushDown();
        }
    }

    private void validateFilterPushDown() {
        if (hasFilterPushDown() == false)
            return;

        Set<TblColRef> filterColumns = Sets.newHashSet();
        TupleFilter.collectColumns(filterPushDown, filterColumns);

        for (TblColRef col : filterColumns) {
            // filter columns must belong to the table
            info.validateColRef(col);
            // filter columns must be returned to satisfy upper layer evaluation (calcite)
            columns.set(col.getColumn().getZeroBasedIndex());
        }

        // un-evaluatable filter must be removed
        if (TupleFilter.isEvaluableRecursively(filterPushDown) == false) {
            Set<TblColRef> unevaluableColumns = Sets.newHashSet();
            filterPushDown = GTUtil.convertFilterUnevaluatable(filterPushDown, info, unevaluableColumns);

            // columns in un-evaluatable filter must be returned without loss so upper layer can do final evaluation
            if (hasAggregation()) {
                for (TblColRef col : unevaluableColumns) {
                    aggrGroupBy.set(col.getColumn().getZeroBasedIndex());
                }
            }
        }
    }

    public boolean hasFilterPushDown() {
        return filterPushDown != null;
    }

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

    public BitSet getColumns() {
        return columns;
    }

    public TupleFilter getFilterPushDown() {
        return filterPushDown;
    }

    public BitSet getAggrGroupBy() {
        return aggrGroupBy;
    }

    public BitSet getAggrMetrics() {
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
