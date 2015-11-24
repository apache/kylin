package org.apache.kylin.aggregation;

import java.util.List;

import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

abstract public class Aggregation {
    
    /* ============================================================================
     * Define
     * ---------------------------------------------------------------------------- */
    
    abstract public DataType getAggregationDataType();
    
    abstract public DataType getResultDataType();
    
    abstract public void validate(MeasureDesc measureDesc) throws IllegalArgumentException;
    
    /* ============================================================================
     * Build
     * ---------------------------------------------------------------------------- */
    
    abstract public DataTypeSerializer<?> getSeralizer();
    
    abstract public MeasureAggregator<?> getAggregator();
 
    abstract public List<TblColRef> getColumnsNeedDictionary(MeasureDesc measureDesc);
    
    abstract public Object reEncodeDictionary(Object value, List<Dictionary<?>> oldDicts, List<Dictionary<?>> newDicts);

    /* ============================================================================
     * Cube Selection
     * ---------------------------------------------------------------------------- */
    
    /* ============================================================================
     * Query
     * ---------------------------------------------------------------------------- */
    
}
