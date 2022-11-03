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

package org.apache.kylin.measure.basic;

import java.math.BigDecimal;

import org.apache.kylin.measure.MeasureAggregator;

/**
 */
@SuppressWarnings("serial")
public class BigDecimalMaxAggregator extends MeasureAggregator<BigDecimal> {

    BigDecimal max = null;

    @Override
    public void reset() {
        max = null;
    }

    @Override
    public void aggregate(BigDecimal value) {
        if (max == null)
            max = value;
        else if (max.compareTo(value) < 0)
            max = value;
    }

    @Override
    public BigDecimal aggregate(BigDecimal value1, BigDecimal value2) {
        if (value1 == null) {
            return value2;
        } else if (value2 == null) {
            return value1;
        }

        if (value1.compareTo(value2) > 0)
            return value1;
        else
            return value2;
    }

    @Override
    public BigDecimal getState() {
        return max;
    }

    @Override
    public int getMemBytesEstimate() {
        return guessBigDecimalMemBytes();
    }
}
