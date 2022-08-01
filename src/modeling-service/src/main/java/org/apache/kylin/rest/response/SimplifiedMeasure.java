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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Setter
@Getter
@EqualsAndHashCode
public class SimplifiedMeasure implements Serializable {

    @EqualsAndHashCode.Exclude
    @JsonProperty("id")
    private int id;
    @JsonProperty("expression")
    private String expression;
    @EqualsAndHashCode.Exclude
    @JsonProperty("name")
    private String name;
    // returnType is concerned in equal comparasion for return type changes in measure
    // see io.kyligence.kap.rest.service.ModelServiceSemanticUpdateTest.testUpdateMeasure_ChangeReturnType
    @JsonProperty("return_type")
    private String returnType;
    @JsonProperty("parameter_value")
    private List<ParameterResponse> parameterValue;
    @EqualsAndHashCode.Exclude
    @JsonProperty("converted_columns")
    private List<ColumnDesc> convertedColumns = new ArrayList<>();
    @EqualsAndHashCode.Exclude
    @JsonProperty("column")
    private String column;
    @EqualsAndHashCode.Exclude
    @JsonProperty("comment")
    private String comment;
    @JsonProperty("configuration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> configuration = Maps.newHashMap();

    public static SimplifiedMeasure fromMeasure(NDataModel.Measure measure) {
        SimplifiedMeasure measureResponse = new SimplifiedMeasure();
        measureResponse.setId(measure.getId());
        measureResponse.setName(measure.getName());
        measureResponse.setExpression(measure.getFunction().getExpression());
        measureResponse.setReturnType(measure.getFunction().getReturnType());
        List<ParameterResponse> parameters = measure.getFunction().getParameters().stream()
                .map(parameterDesc -> new ParameterResponse(parameterDesc.getType(), parameterDesc.getValue()))
                .collect(Collectors.toList());
        measureResponse.setConfiguration(measure.getFunction().getConfiguration());
        measureResponse.setParameterValue(parameters);
        measureResponse.setColumn(measure.getColumn());
        measureResponse.setComment(measure.getComment());
        return measureResponse;
    }

    public NDataModel.Measure toMeasure() {
        NDataModel.Measure measure = new NDataModel.Measure();
        measure.setId(getId());
        measure.setName(getName());
        measure.setColumn(getColumn());
        measure.setComment(getComment());
        FunctionDesc functionDesc = new FunctionDesc();
        functionDesc.setReturnType(getReturnType());
        functionDesc.setExpression(getExpression());
        functionDesc.setConfiguration(configuration);
        List<ParameterResponse> parameterResponseList = getParameterValue();

        // transform parameter response to parameter desc
        List<ParameterDesc> parameterDescs = parameterResponseList.stream().map(parameterResponse -> {
            ParameterDesc parameterDesc = new ParameterDesc();
            parameterDesc.setType(parameterResponse.getType());
            parameterDesc.setValue(parameterResponse.getValue());
            return parameterDesc;
        }).collect(Collectors.toList());
        functionDesc.setParameters(parameterDescs);
        measure.setFunction(functionDesc);
        return measure;
    }

    public void changeTableAlias(String oldAlias, String newAlias) {
        for (val parameter : parameterValue) {
            String table = parameter.getValue().split("\\.")[0];
            if (oldAlias.equalsIgnoreCase(table)) {
                String column = parameter.getValue().split("\\.")[1];
                parameter.setValue(newAlias + "." + column);
            }
        }
    }
}
