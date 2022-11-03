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

/**
 * Bitmap-based distinct count UDAF, called by calcite runtime.
 */
public class BitmapDistinctCountAggFunc {

    public static BitmapAggregator init() {
        return new BitmapAggregator();
    }

    public static BitmapAggregator add(BitmapAggregator agg, Object value) {
        agg.aggregate((BitmapCounter) value);
        return agg;
    }

    public static BitmapAggregator merge(BitmapAggregator agg, Object value) {
        BitmapAggregator agg2 = (BitmapAggregator) value;
        if (agg2.getState() == null) {
            return agg;
        }
        return add(agg, agg2.getState());
    }

    public static Object result(BitmapAggregator agg) {
        BitmapCounter finalState = agg.getState();
        return finalState == null ? 0 : finalState.getCount();
    }
}
