package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.measure.MeasureAggregator;

public interface IGTCodeSystem {
    
    void init(GTInfo info);

    IFilterCodeSystem<ByteArray> getFilterCodeSystem();
    
    /** Return the length of code starting at the specified buffer, buffer position must not change after return */
    int codeLength(int col, ByteBuffer buf);
    
    /**
     * Encode a value into code.
     * 
     * @throws IllegalArgumentException if the value is not in dictionary
     */
    void encodeColumnValue(int col, Object value, ByteBuffer buf) throws IllegalArgumentException;
    
    /**
     * Encode a value into code, with option to floor rounding -1, no rounding 0,  or ceiling rounding 1
     * 
     * @throws IllegalArgumentException
     * - if rounding=0 and the value is not in dictionary
     * - if rounding=-1 and there's no equal or smaller value in dictionary
     * - if rounding=1 and there's no equal or bigger value in dictionary
     */
    void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) throws IllegalArgumentException;
    
    /** Decode a code into value */
    Object decodeColumnValue(int col, ByteBuffer buf);
    
    /** Return an aggregator for metrics */
    MeasureAggregator<?> newMetricsAggregator(String aggrFunction, int col);
    
}
