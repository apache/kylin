package com.kylinolap.metadata.realization;

import java.util.Collection;

import com.kylinolap.metadata.filter.TupleFilter;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.JoinDesc;
import com.kylinolap.metadata.model.TblColRef;

/**
 * Created by Hongbin Ma(Binmahone) on 1/8/15.
 */
public class SQLDigest {
    public String factTable;
    public TupleFilter filter;
    public Collection<JoinDesc> joinDescs;
    public Collection<TblColRef> allColumns;
    public Collection<TblColRef> groupbyColumns;
    public Collection<TblColRef> filterColumns;
    public Collection<TblColRef> metricColumns;
    public Collection<FunctionDesc> aggregations;

    public SQLDigest(String factTable, TupleFilter filter, Collection<JoinDesc> joinDescs, Collection<TblColRef> allColumns, //
            Collection<TblColRef> groupbyColumns, Collection<TblColRef> filterColumns, Collection<TblColRef> aggregatedColumns, Collection<FunctionDesc> aggregateFunnc) {
        this.factTable = factTable;
        this.filter = filter;
        this.joinDescs = joinDescs;
        this.allColumns = allColumns;
        this.groupbyColumns = groupbyColumns;
        this.filterColumns = filterColumns;
        this.metricColumns = aggregatedColumns;
        this.aggregations = aggregateFunnc;
    }

    @Override
    public String toString() {
        return "fact table " + this.factTable + "," + //
                "group by " + this.groupbyColumns + "," + //
                "filter on " + this.filterColumns + "," + //
                "with aggregates" + this.aggregations + ".";
    }
}
