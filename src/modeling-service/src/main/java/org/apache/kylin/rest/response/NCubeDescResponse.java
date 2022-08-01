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
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class NCubeDescResponse implements Serializable {
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("name")
    private String name;
    @JsonProperty("dimensions")
    private List<Dimension3X> dimensions;
    @JsonProperty("measures")
    private List<Measure3X> measures;
    @JsonProperty("aggregation_groups")
    private List<AggGroupResponse> aggregationGroups;

    @Data
    public static class Measure3X implements Serializable {
        @JsonProperty("name")
        private String name;
        @JsonProperty("description")
        private String description;
        @JsonProperty("function")
        private FunctionDesc3X functionDesc3X;

        public Measure3X() {
        }

        public Measure3X(NDataModel.Measure measure) {
            this.setName(measure.getName());
            this.setDescription("");
            this.setFunctionDesc3X(new FunctionDesc3X(measure.getFunction()));
        }
    }

    @Data
    public static class Dimension3X implements Serializable {
        @JsonProperty("name")
        private String name;
        @JsonProperty("table")
        private String table;
        @JsonProperty("column")
        private String column;
        @JsonProperty("derived")
        private List<String> derived;

        public Dimension3X() {
        }

        public Dimension3X(NDataModel.NamedColumn namedColumn, boolean isDerived) {
            this.name = namedColumn.getName();
            this.table = namedColumn.getAliasDotColumn().split("\\.")[0].toUpperCase(Locale.ROOT).trim();
            String columnName = namedColumn.getAliasDotColumn().split("\\.")[1].toUpperCase(Locale.ROOT).trim();
            if (!isDerived) {
                this.column = columnName;
                this.derived = null;
            } else {
                this.column = null;
                this.derived = Collections.singletonList(columnName);
            }

        }
    }

    @Data
    public static class FunctionDesc3X implements Serializable {
        @JsonProperty("expression")
        private String expression;
        @JsonProperty("parameter")
        private ParameterDesc3X parameter;
        @JsonProperty("returntype")
        private String returnType;

        public FunctionDesc3X() {
        }

        public FunctionDesc3X(FunctionDesc functionDesc) {
            this.setParameter(ParameterDesc3X.convert(functionDesc.getParameters()));
            this.setExpression(functionDesc.getExpression());
            this.setReturnType(functionDesc.getReturnType());
        }
    }

    @Data
    public static class ParameterDesc3X implements Serializable {
        @Getter
        @Setter
        @JsonProperty("type")
        private String type;
        @Getter
        @Setter
        @JsonProperty("value")
        private String value;

        @JsonProperty("next_parameter")
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private ParameterDesc3X nextParameter;

        public static ParameterDesc3X convert(ParameterDesc parameterDesc) {
            ParameterDesc3X parameterDesc3X = new ParameterDesc3X();
            parameterDesc3X.setType(parameterDesc.getType());
            parameterDesc3X.setValue(parameterDesc.getValue());
            return parameterDesc3X;
        }

        public static ParameterDesc3X convert(List<ParameterDesc> parameterDescs) {
            if (CollectionUtils.isEmpty(parameterDescs)) {
                return new ParameterDesc3X();
            }

            ParameterDesc3X head = null;
            ParameterDesc3X tail = null;
            for (ParameterDesc parameterDesc : parameterDescs) {
                if (null == head) {
                    head = convert(parameterDesc);
                    tail = head;
                    continue;
                }

                tail.nextParameter = convert(parameterDesc);
            }

            return head;
        }
    }

}
