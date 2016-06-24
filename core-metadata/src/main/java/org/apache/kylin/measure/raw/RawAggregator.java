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

package org.apache.kylin.measure.raw;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.measure.MeasureAggregator;

/**
 * RAW data Aggregator
 */
public class RawAggregator extends MeasureAggregator<List<ByteArray>> {

    List<ByteArray> list = null;

    @Override
    public void reset() {
        list = null;
    }

    @Override
    public void aggregate(List<ByteArray> value) {
        if (value != null) {
            if (list == null) {
                list = new ArrayList<>(value.size());
            }
            list.addAll(value);
        }
    }

    @Override
    public List<ByteArray> getState() {
        return list;
    }

    @Override
    public int getMemBytesEstimate() {
        int bytes = 0;
        if (list != null) {
            for (ByteArray array : list) {
                bytes += array.length() + 1;
            }
        }
        return bytes;
    }

}
