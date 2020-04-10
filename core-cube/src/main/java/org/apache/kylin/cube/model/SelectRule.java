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

package org.apache.kylin.cube.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SelectRule implements Serializable {
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("hierarchy_dims")
    public String[][] hierarchyDims;
    @JsonProperty("mandatory_dims")
    public String[] mandatoryDims;
    @JsonProperty("joint_dims")
    public String[][] jointDims;
    @JsonProperty("dim_cap")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer dimCap;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SelectRule that = (SelectRule) o;
        if (hierarchyDims != that.hierarchyDims) {
            if (hierarchyDims == null || that.hierarchyDims == null) {
                return false;
            } else if (!IntStream.range(0, hierarchyDims.length)
                    .allMatch(i -> Arrays.equals(hierarchyDims[i], that.hierarchyDims[i]))) {
                return false;
            }
        }

        if (jointDims != that.jointDims) {
            if (jointDims == null || that.jointDims == null) {
                return false;
            } else if (!IntStream.range(0, jointDims.length)
                    .allMatch(i -> Arrays.equals(jointDims[i], that.jointDims[i]))) {
                return false;
            }
        }
        return Arrays.equals(mandatoryDims, that.mandatoryDims) && Objects.equals(dimCap, that.dimCap);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(dimCap);
        result = 31 * result + Arrays.hashCode(hierarchyDims);
        result = 31 * result + Arrays.hashCode(mandatoryDims);
        result = 31 * result + Arrays.hashCode(jointDims);
        return result;
    }
}
