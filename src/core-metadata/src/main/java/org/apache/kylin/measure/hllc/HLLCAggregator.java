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

package org.apache.kylin.measure.hllc;

import org.apache.kylin.measure.MeasureAggregator;

/**
 */
@SuppressWarnings("serial")
public class HLLCAggregator extends MeasureAggregator<HLLCounter> {

    final int precision;
    HLLCounter sum = null;

    public HLLCAggregator(int precision) {
        this.precision = precision;
    }

    @Override
    public void reset() {
        sum = null;
    }

    @Override
    public void aggregate(HLLCounter value) {
        if (sum == null)
            sum = new HLLCounter(value);
        else
            sum.merge(value);
    }

    @Override
    public HLLCounter aggregate(HLLCounter value1, HLLCounter value2) {
        HLLCounter result = new HLLCounter(value1);
        result.merge(value2);
        return result;
    }

    @Override
    public HLLCounter getState() {
        return sum;
    }

    @Override
    public int getMemBytesEstimate() {
        // 1024 + 60 returned by AggregationCacheMemSizeTest
        return 8 // aggregator obj shell
                + 4 // precision
                + 8 // ref to HLLC
                + 8 // HLLC obj shell
                + 32 + (1 << precision); // HLLC internal
    }

    public void add(String s) {
        if (sum == null) {
            sum = new HLLCounter(precision);
        }
        sum.add(s);
    }

}
