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

import java.util.List;

import org.apache.kylin.measure.ParamAsMeasureCount;

/**
 * BitmapIntersectDistinctCountAggFunc is an UDAF used for calculating the intersection of two or more bitmaps
 * Usage:   intersect_count(columnToCount, columnToFilter, filterList)
 * Example: intersect_count(uuid, event, array['A', 'B', 'C']), meaning find the count of uuid in all A/B/C 3 bitmaps
 *          requires an bitmap count distinct measure of uuid, and an dimension of event
 */
public class BitmapIntersectValueAggFunc implements ParamAsMeasureCount {

    @Override
    public int getParamAsMeasureCount() {
        return -2;
    }

    public static RetentionPartialResult init() {
        return new RetentionPartialResult();
    }

    public static RetentionPartialResult add(RetentionPartialResult result, Object value, Object key, List keyList) {
        result.add(key, keyList, value);
        return result;
    }

    public static RetentionPartialResult merge(RetentionPartialResult result, Object value, Object key, List keyList) {
        return add(result, value, key, keyList);
    }

    public static String result(RetentionPartialResult result) {
        return result.valueResult();
    }
}

