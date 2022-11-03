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

public class PercentileAggFunc {
    public static PercentileCounter init() {
        return null;
    }

    public static PercentileCounter add(PercentileCounter counter, Object v, Object r) {
        PercentileCounter c = (PercentileCounter) v;
        Number n = (Number) r;
        if (counter == null) {
            counter = new PercentileCounter(c.compression, n.doubleValue());
        }
        counter.merge(c);
        return counter;
    }

    public static PercentileCounter merge(PercentileCounter counter0, PercentileCounter counter1) {
        counter0.merge(counter1);
        return counter0;
    }

    public static double result(PercentileCounter counter) {
        return counter == null ? 0L : counter.getResultEstimate();
    }
}
