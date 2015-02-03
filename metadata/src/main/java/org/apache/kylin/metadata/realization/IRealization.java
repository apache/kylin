package org.apache.kylin.metadata.realization;

import java.util.List;

import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

public interface IRealization {

    public boolean isCapable(SQLDigest digest);

    /**
     * Given the features of a query, return an integer indicating how capable the realization
     * is to answer the query.
     *
     * @return -1 if the realization cannot fulfill the query;
     *         or a number between 0-100 if the realization can answer the query, the smaller
     *         the number, the more efficient the realization.
     *         Especially,
     *           0 - means the realization has the exact result pre-calculated, no less no more;
     *         100 - means the realization will scan the full table with little or no indexing.
     */
    public int getCost(SQLDigest digest);

    /**
     * Get whether this specific realization is a cube or InvertedIndex
     *
     * @return
     */
    public RealizationType getType();

    public String getFactTable();

    public List<TblColRef> getAllColumns();

    public List<MeasureDesc> getMeasures();

    public boolean isReady();

    public String getName();

    public String getCanonicalName();
    
    public String getProjectName();
    
    public void setProjectName(String prjName);

}
