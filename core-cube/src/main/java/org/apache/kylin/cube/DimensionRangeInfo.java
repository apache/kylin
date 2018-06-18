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

package org.apache.kylin.cube;

import java.util.Map;

import org.apache.kylin.metadata.datatype.DataTypeOrder;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DimensionRangeInfo {

    private static final Logger logger = LoggerFactory.getLogger(DimensionRangeInfo.class);

    public static Map<String, DimensionRangeInfo> mergeRangeMap(DataModelDesc model, Map<String, DimensionRangeInfo> m1,
            Map<String, DimensionRangeInfo> m2) {

        if (!m1.keySet().equals(m2.keySet())) {
            logger.warn("Merging incompatible maps of DimensionRangeInfo, keys in m1 " + m1.keySet() + ", keys in m2 "
                    + m2.keySet());
        }

        Map<String, DimensionRangeInfo> result = Maps.newHashMap();
        
        for (String colId : m1.keySet()) {
            if (!m2.containsKey(colId))
                continue;

            DimensionRangeInfo r1 = m1.get(colId);
            DimensionRangeInfo r2 = m2.get(colId);
            
            DimensionRangeInfo newR;
            if (r1.getMin() == null && r1.getMax() == null) {
                newR = r2; // when r1 is all null or has 0 records
            } else if (r2.getMin() == null && r2.getMax() == null) {
                newR = r1; // when r2 is all null or has 0 records
            } else {
                DataTypeOrder order = model.findColumn(colId).getType().getOrder();
                String newMin = order.min(r1.getMin(), r2.getMin());
                String newMax = order.max(r1.getMax(), r2.getMax());
                newR = new DimensionRangeInfo(newMin, newMax);
            }
            
            result.put(colId, newR);
        }
        
        return result;
    }
    
    // ============================================================================

    @JsonProperty("min")
    private String min;

    @JsonProperty("max")
    private String max;

    public DimensionRangeInfo() {}

    public DimensionRangeInfo(String min, String max) {
        if (min == null && max != null || min != null && max == null)
            throw new IllegalStateException();
        
        this.min = min;
        this.max = max;
    }

    public String getMin() {
        return min;
    }

    public String getMax() {
        return max;
    }
    
    @Override
    public String toString() {
        return "[" + min + ", " + max + "]";
    }

}
