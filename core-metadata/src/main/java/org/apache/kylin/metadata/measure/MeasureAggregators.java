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

package org.apache.kylin.metadata.measure;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

/**
 * @author yangli9
 * 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MeasureAggregators {

    private MeasureDesc[] descs;
    private MeasureAggregator[] aggs;

    public MeasureAggregators(Collection<MeasureDesc> measureDescs) {
        this((MeasureDesc[]) measureDescs.toArray(new MeasureDesc[measureDescs.size()]));
    }

    public MeasureAggregators(MeasureDesc... measureDescs) {
        descs = measureDescs;
        aggs = new MeasureAggregator[descs.length];

        Map<String, Integer> measureIndexMap = new HashMap<String, Integer>();
        for (int i = 0; i < descs.length; i++) {
            FunctionDesc func = descs[i].getFunction();
            aggs[i] = MeasureAggregator.create(func.getExpression(), func.getReturnType());
            measureIndexMap.put(descs[i].getName(), i);
        }
        // fill back dependent aggregator
        for (int i = 0; i < descs.length; i++) {
            String depMsrRef = descs[i].getDependentMeasureRef();
            if (depMsrRef != null) {
                int index = measureIndexMap.get(depMsrRef);
                aggs[i].setDependentAggregator(aggs[index]);
            }
        }
    }

    public void reset() {
        for (int i = 0; i < aggs.length; i++) {
            aggs[i].reset();
        }
    }

    public void aggregate(Object[] values) {
        assert values.length == descs.length;

        for (int i = 0; i < descs.length; i++) {
            aggs[i].aggregate(values[i]);
        }
    }

    public void collectStates(Object[] states) {
        for (int i = 0; i < descs.length; i++) {
            states[i] = aggs[i].getState();
        }
    }

}
