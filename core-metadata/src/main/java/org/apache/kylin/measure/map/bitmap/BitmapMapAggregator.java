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

package org.apache.kylin.measure.map.bitmap;

import org.apache.kylin.measure.MeasureAggregator;

public class BitmapMapAggregator extends MeasureAggregator<BitmapCounterMap> {

    private static final BitmapCounterMapFactory bitmapFactory = RoaringBitmapCounterMapFactory.INSTANCE;

    private BitmapCounterMap sum;

    @Override
    public void reset() {
        sum = null;
    }

    @Override
    public void aggregate(BitmapCounterMap value) {
        // Here we optimize for case when group only has 1 value. In such situation, no
        // aggregation is needed, so we just keep a reference to the first value, saving
        // the cost of deserialization and merging.
        if (sum == null) {
            sum = value;
            return;
        }

        sum.orWith(value);
    }

    @Override
    public BitmapCounterMap aggregate(BitmapCounterMap value1, BitmapCounterMap value2) {
        BitmapCounterMap merged = bitmapFactory.newBitmapMap();
        if (value1 != null) {
            merged.orWith(value1);
        }
        if (value2 != null) {
            merged.orWith(value2);
        }
        return merged;
    }

    @Override
    public BitmapCounterMap getState() {
        return sum;
    }

    @Override
    public int getMemBytesEstimate() {
        return sum == null ? 0 : sum.getMemBytes();
    }
}
