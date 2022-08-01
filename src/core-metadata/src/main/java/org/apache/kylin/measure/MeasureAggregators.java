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

package org.apache.kylin.measure;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

/**
 */
@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
public class MeasureAggregators implements Serializable {

    private final MeasureAggregator[] aggs;
    private final int descLength;

    public MeasureAggregators(MeasureAggregator... aggs) {
        this.descLength = aggs.length;
        this.aggs = aggs;
    }

    public MeasureAggregators(Collection<MeasureDesc> measureDescs) {
        this((MeasureDesc[]) measureDescs.toArray(new MeasureDesc[measureDescs.size()]));
    }

    public MeasureAggregators(MeasureDesc... measureDescs) {
        descLength = measureDescs.length;
        aggs = new MeasureAggregator[descLength];

        Map<String, Integer> measureIndexMap = new HashMap<String, Integer>();
        for (int i = 0; i < descLength; i++) {
            FunctionDesc func = measureDescs[i].getFunction();
            aggs[i] = func.getMeasureType().newAggregator();
            measureIndexMap.put(measureDescs[i].getName(), i);
        }
        // fill back dependent aggregator
        for (int i = 0; i < descLength; i++) {
            String depMsrRef = measureDescs[i].getDependentMeasureRef();
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
        assert values.length == descLength;

        for (int i = 0; i < descLength; i++) {
            aggs[i].aggregate(values[i]);
        }
    }

    public void aggregate(Object[] values, int[] measures) {
        assert values.length == descLength;
        for (int i = 0; i < measures.length; i++) {
            aggs[measures[i]].aggregate(values[measures[i]]);
        }
    }

    public void aggregate(Object[] values1, Object[] values2, Object[] result) {
        assert values1.length == values2.length && values2.length == descLength && values1.length == result.length;

        for (int i = 0; i < descLength; i++) {
            result[i] = aggs[i].aggregate(values1[i], values2[i]);
        }

    }

    public void aggregate(Object[] values1, Object[] values2, Object[] result, boolean[] aggrMask) {
        assert values1.length == values2.length && values2.length == descLength && values1.length == result.length
                && result.length == aggrMask.length;
        for (int i = 0; i < descLength; i++) {
            if (aggrMask[i]) {
                result[i] = aggs[i].aggregate(values1[i], values2[i]);
            }
        }
    }

    public void collectStates(Object[] states) {
        for (int i = 0; i < descLength; i++) {
            states[i] = aggs[i].getState();
        }
    }
}
