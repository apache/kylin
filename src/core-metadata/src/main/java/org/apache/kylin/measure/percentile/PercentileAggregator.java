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

package org.apache.kylin.measure.percentile;

import org.apache.kylin.measure.MeasureAggregator;

public class PercentileAggregator extends MeasureAggregator<PercentileCounter> {
    final double compression;
    PercentileCounter sum = null;

    public PercentileAggregator(double compression) {
        this.compression = compression;
    }

    @Override
    public void reset() {
        sum = null;
    }

    @Override
    public void aggregate(PercentileCounter value) {
        if (sum == null)
            sum = new PercentileCounter(value);
        else
            sum.merge(value);
    }

    @Override
    public PercentileCounter aggregate(PercentileCounter value1, PercentileCounter value2) {
        PercentileCounter merged = new PercentileCounter(value1);
        merged.merge(value2);
        return merged;
    }

    @Override
    public PercentileCounter getState() {
        return sum;
    }

    @Override
    public int getMemBytesEstimate() {
        // 10K as upbound
        // Test on random double data, 20 tDigest, each has 5000000 doubles. Finally merged into one tDigest.
        // Before compress: 10309 bytes
        // After compress: 8906 bytes
        return 10 * 1024;
    }
}
