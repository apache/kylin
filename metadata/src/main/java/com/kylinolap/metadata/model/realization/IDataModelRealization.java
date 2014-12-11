package com.kylinolap.metadata.model.realization;

import java.util.Collection;

import com.kylinolap.metadata.model.JoinDesc;

public interface IDataModelRealization {

    /**
     * Given the features of a query, return an integer indicating how capable the realization
     * is to answer the query.
     * 
     * @return -1 if the realization cannot fulfill the query;
     *         or a number between 0-100 if the realization can answer the query, the smaller
     *         the number, the more efficient the realization. Especially,
     *         0   - means the realization has the exact result pre-calculated, no less no more;
     *         100 - means the realization will scan the full table with little or no indexing.
     */
    int getCost(String factTable, Collection<JoinDesc> joins, Collection<TblColRef> allColumns, //
            Collection<FunctionDesc> aggrFunctions);
}
