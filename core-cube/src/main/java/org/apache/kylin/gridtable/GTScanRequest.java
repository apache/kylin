package org.apache.kylin.gridtable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
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
    private double aggrCacheGB = 0; // no limit

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
        if (!hasFilterPushDown())
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
        if (!TupleFilter.isEvaluableRecursively(filterPushDown)) {
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
        return decorateScanner(scanner, true, true);//by default do not check mem
    }

    /**
     * doFilter,doAggr,doMemCheck are only for profiling use.
     * in normal cases they are all true.
     * 
     * Refer to CoprocessorBehavior for explanation
     */
    public IGTScanner decorateScanner(IGTScanner scanner, boolean doFilter, boolean doAggr) throws IOException {
        IGTScanner result = scanner;
        if (!doFilter) { //Skip reading this section if you're not profiling! 
            lookAndForget(result);
            return new EmptyGTScanner();
        } else {

            if (this.hasFilterPushDown()) {
                result = new GTFilterScanner(result, this);
            }

            if (!doAggr) {//Skip reading this section if you're not profiling! 
                lookAndForget(result);
                return new EmptyGTScanner();
            }

            if (this.allowPreAggregation && this.hasAggregation()) {
                result = new GTAggregateScanner(result, this);
            }
            return result;
        }
    }

    //touch every byte of the cell so that the cost of scanning will be truly reflected
    private void lookAndForget(IGTScanner scanner) {
        byte meaninglessByte = 0;
        for (GTRecord gtRecord : scanner) {
            for (ByteArray col : gtRecord.getInternal()) {
                if (col != null) {
                    int endIndex = col.offset() + col.length();
                    for (int i = col.offset(); i < endIndex; ++i) {
                        meaninglessByte += col.array()[i];
                    }
                }
            }
        }
        System.out.println("meaningless byte is now " + meaninglessByte);
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

    public List<GTRecord> getFuzzyKeys() {
        return range.fuzzyKeys;
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
    
    public double getAggrCacheGB() {
        return aggrCacheGB;
    }
    
    public void setAggrCacheGB(double gb) {
        this.aggrCacheGB = gb;
    }

    @Override
    public String toString() {
        return "GTScanRequest [range=" + range + ", columns=" + columns + ", filterPushDown=" + filterPushDown + ", aggrGroupBy=" + aggrGroupBy + ", aggrMetrics=" + aggrMetrics + ", aggrMetricsFuncs=" + Arrays.toString(aggrMetricsFuncs) + "]";
    }

}
