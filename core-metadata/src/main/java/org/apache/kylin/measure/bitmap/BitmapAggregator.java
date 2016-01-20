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

package org.apache.kylin.measure.bitmap;

import org.apache.kylin.measure.MeasureAggregator;

/**
 * Created by sunyerui on 15/12/2.
 */
public class BitmapAggregator extends MeasureAggregator<BitmapCounter> {

    private BitmapCounter sum = null;

    @Override
    public void reset() {
        sum = null;
    }

    @Override
    public void aggregate(BitmapCounter value) {
        if (sum == null) {
            sum = new BitmapCounter(value);
        } else {
            sum.merge(value);
        }
    }

    @Override
    public BitmapCounter getState() {
        return sum;
    }

    @Override
    public int getMemBytesEstimate() {
        if (sum == null) {
            return Integer.MIN_VALUE;
        } else {
            return sum.getMemBytes();
        }
    }
}
