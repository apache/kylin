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

package org.apache.kylin.rest.response;

import java.io.Serializable;

import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class AggGroupResponse implements Serializable {
    @JsonProperty("includes")
    private String[] includes;
    @JsonProperty("select_rule")
    private AggSelectRule aggSelectRule;

    public AggGroupResponse() {
    }

    public AggGroupResponse(NDataModel dataModel, NAggregationGroup aggregationGroup) {
        includes = intArray2StringArray(aggregationGroup.getIncludes(), dataModel);
        aggSelectRule = new AggSelectRule(dataModel, aggregationGroup.getSelectRule());
    }

    @Data
    public static class AggSelectRule implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("hierarchy_dims")
        public String[][] hierarchyDims;
        @JsonProperty("mandatory_dims")
        public String[] mandatoryDims;
        @JsonProperty("joint_dims")
        public String[][] jointDims;

        public AggSelectRule() {
        }

        public AggSelectRule(NDataModel dataModel, SelectRule selectRule) {
            hierarchyDims = intArray2StringArray(selectRule.getHierarchyDims(), dataModel);
            mandatoryDims = intArray2StringArray(selectRule.getMandatoryDims(), dataModel);
            jointDims = intArray2StringArray(selectRule.getJointDims(), dataModel);
        }
    }

    public static String[] intArray2StringArray(Integer[] ints, NDataModel dataModel) {
        int len = ints == null ? 0 : ints.length;
        String[] res;
        if (len > 0) {
            res = new String[len];
            for (int i = 0; i < len; ++i) {
                res[i] = dataModel.getEffectiveDimensions().get(ints[i]).getIdentity();
            }
        } else {
            res = new String[0];
        }
        return res;
    }

    public static String[][] intArray2StringArray(Integer[][] ints, NDataModel dataModel) {
        int p1, p2;
        String[][] res;
        p1 = ints == null ? 0 : ints.length;
        if (p1 > 0) {
            res = new String[p1][];
            for (int i = 0; i < p1; ++i) {
                p2 = ints[i].length;
                res[i] = new String[p2];
                for (int j = 0; j < p2; ++j) {
                    res[i][j] = dataModel.getEffectiveDimensions().get(ints[i][j]).getIdentity();
                }
            }
        } else {
            res = new String[0][0];
        }
        return res;
    }

}
