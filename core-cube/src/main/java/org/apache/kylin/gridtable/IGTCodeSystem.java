package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.aggregation.MeasureAggregator;
import org.apache.kylin.common.util.ImmutableBitSet;

public interface IGTCodeSystem {

    void init(GTInfo info);

    IGTComparator getComparator();

    /** Return the length of code starting at the specified buffer, buffer position must not change after return */
    int codeLength(int col, ByteBuffer buf);

    /** Return the max possible length of a column */
    int maxCodeLength(int col);

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

    /** Return aggregators for metrics */
    MeasureAggregator<?>[] newMetricsAggregators(ImmutableBitSet columns, String[] aggrFunctions);

}
