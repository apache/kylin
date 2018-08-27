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

import org.apache.kylin.measure.ParamAsMeasureCount;

public class AucAggFunc implements ParamAsMeasureCount {

    public static AucCounter init() {
        return null;
    }

    public static AucCounter add(AucCounter counter, Object t, Object p) {
        if (counter == null) {
            counter = new AucCounter();
        }
        counter.addTruth(t);
        counter.addPred(p);
        return counter;
    }

    public static AucCounter merge(AucCounter counter0, AucCounter counter1) {
        counter0.merge(counter1);
        return counter0;
    }

    public static double result(AucCounter counter) {
        return counter == null ? -1D : counter.auc();
    }

    @Override
    public int getParamAsMeasureCount() {
        return 2;
    }
}
