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

package org.apache.kylin.metadata.cube.model;

import java.io.Serializable;

import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DimensionRangeInfo implements Serializable {
    @JsonProperty("min")
    private String min;

    @JsonProperty("max")
    private String max;

    public DimensionRangeInfo(String min, String max) {
        if (min == null && max != null || min != null && max == null)
            throw new IllegalStateException();

        this.min = min;
        this.max = max;
    }

    public String getMin() {
        return min;
    }

    public void setMin(String min) {
        this.min = min;
    }

    public String getMax() {
        return max;
    }

    public void setMax(String max) {
        this.max = max;
    }

    public DimensionRangeInfo merge(DimensionRangeInfo dimensionRangeInfo, DataType dataType) {
        String minValue = dataType.compare(this.min, dimensionRangeInfo.getMin()) < 0 ? this.min
                : dimensionRangeInfo.getMin();
        String maxValue = dataType.compare(this.max, dimensionRangeInfo.getMax()) > 0 ? this.max
                : dimensionRangeInfo.getMax();
        return new DimensionRangeInfo(minValue, maxValue);
    }
}
