package com.kylinolap.metadata.realization;

import java.util.Collection;
import java.util.List;

import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.JoinDesc;
import com.kylinolap.metadata.model.MeasureDesc;
import com.kylinolap.metadata.model.TblColRef;

public interface IRealization {

    /**
     * Given the features of a query, return an integer indicating how capable the realization
     * is to answer the query.
     *
     * @return -1 if the realization cannot fulfill the query;
     * or a number between 0-100 if the realization can answer the query, the smaller
     * the number, the more efficient the realization. Especially,
     * 0   - means the realization has the exact result pre-calculated, no less no more;
     * 100 - means the realization will scan the full table with little or no indexing.
     */
    public int getCost(String factTable, Collection<JoinDesc> joins, Collection<TblColRef> allColumns, //
            Collection<FunctionDesc> aggrFunctions);

    /**
     * Get whether this specific realization is a cube or InvertedIndex
     *
     * @return
     */
    public RealizationType getType();

    public List<TblColRef> getAllColumns();

    public boolean isReady();

    public String getName();

    public String getCanonicalName(String name);

    public String getFactTable();

    public List<MeasureDesc> getMeasures();
}
