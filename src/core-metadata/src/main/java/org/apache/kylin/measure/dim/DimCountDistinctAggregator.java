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

package org.apache.kylin.measure.dim;

import org.apache.kylin.measure.MeasureAggregator;

/**
 */
@SuppressWarnings("serial")
public class DimCountDistinctAggregator extends MeasureAggregator<DimCountDistinctCounter> {

    DimCountDistinctCounter state = null;

    public DimCountDistinctAggregator() {
    }

    @Override
    public void reset() {
        state = null;
    }

    @Override
    public void aggregate(DimCountDistinctCounter value) {
        if (state == null)
            state = new DimCountDistinctCounter(value);
        else
            state.addAll(value);
    }

    @Override
    public DimCountDistinctCounter aggregate(DimCountDistinctCounter value1, DimCountDistinctCounter value2) {
        DimCountDistinctCounter result = new DimCountDistinctCounter(value1);
        result.addAll(value2);
        return result;
    }

    @Override
    public DimCountDistinctCounter getState() {
        return state;
    }

    @Override
    public int getMemBytesEstimate() {
        return state.estimateSize();
    }

}
