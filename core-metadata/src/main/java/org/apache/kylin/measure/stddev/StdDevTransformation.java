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

package org.apache.kylin.measure.stddev;

import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureTransformation;

public class StdDevTransformation extends MeasureAggregator<StdDevCounter>
        implements MeasureTransformation<StdDevCounter> {

    StdDevCounter counter;

    @Override
    public void reset() {
        counter = null;
    }

    @Override
    public void aggregate(StdDevCounter value) {
        if (counter == null)
            counter = new StdDevCounter(value);
        else
            counter.merge(value);
    }

    @Override
    public StdDevCounter aggregate(StdDevCounter value1, StdDevCounter value2) {
        StdDevCounter merged = new StdDevCounter(value1);
        merged.merge(value2);
        return merged;
    }

    @Override
    public StdDevCounter getState() {
        return counter;
    }

    @Override
    public int getMemBytesEstimate() {
        return 8 // aggregator obj shell
                + 8 // ref to StdDevCounter
                + 8 // StdDevCounter obj shell
                + 24; // StdDevCounter internal
    }

    @Override
    public StdDevCounter transformMeasure(Object measureValue) {
        assert measureValue instanceof Number;
        double value = ((Number) measureValue).doubleValue();
        return new StdDevCounter(value);
    }
}