package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.measure.MeasureAggregator;

public interface IGTCodeSystem {
    
    void init(GTInfo info);

    IFilterCodeSystem<ByteArray> getFilterCodeSystem();
    
    /** return the length of code starting at the specified buffer, buffer position must not change after return */
    int codeLength(int col, ByteBuffer buf);
    
    /** encode a value into code */
    void encodeColumnValue(int col, Object value, ByteBuffer buf);
    
    /** encode a value into code, with option to floor rounding -1, no rounding 0,  or ceiling rounding 1 */
    void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf);
    
    /** decode a code into value */
    Object decodeColumnValue(int col, ByteBuffer buf);
    
    /** return an aggregator for metrics */
    MeasureAggregator<?> newMetricsAggregator(String aggrFunction, int col);
    
}
