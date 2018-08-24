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

package org.apache.kylin.measure.auc;

import org.apache.kylin.measure.MeasureAggregator;

public class AucAggregator extends MeasureAggregator<AucCounter> {
    AucCounter auc = null;

    public AucAggregator() {
    }

    @Override
    public void reset() {
        auc = null;
    }

    @Override
    public void aggregate(AucCounter value) {
        if (auc == null)
            auc = new AucCounter(value);
        else
            auc.merge(value);
    }

    @Override
    public AucCounter aggregate(AucCounter value1, AucCounter value2) {
        if (value1 == null) {
            return value2;
        } else if (value2 == null) {
            return value1;
        }
        value1.merge(value2);
        return value1;
    }

    @Override
    public AucCounter getState() {
        return auc;
    }

    @Override
    public int getMemBytesEstimate() {
        return auc.getTruth().size() * 4 + auc.getPred().size() * 4;
    }
}
