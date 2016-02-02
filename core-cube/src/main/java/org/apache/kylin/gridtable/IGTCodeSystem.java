/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.measure.MeasureAggregator;

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
