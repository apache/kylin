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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class MeasureDesc implements Serializable {

    @JsonProperty("name")
    private String name;
    @JsonProperty("function")
    private FunctionDesc function;
    @JsonProperty("dependent_measure_ref")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Deprecated
    private String dependentMeasureRef;
    @JsonProperty("column")
    private String column;
    @JsonProperty("comment")
    private String comment;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FunctionDesc getFunction() {
        return function;
    }

    public void setFunction(FunctionDesc function) {
        this.function = function;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Deprecated
    public String getDependentMeasureRef() {
        return dependentMeasureRef;
    }

    @Deprecated
    public void setDependentMeasureRef(String dependentMeasureRef) {
        this.dependentMeasureRef = dependentMeasureRef;
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, dependentMeasureRef);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        MeasureDesc that = (MeasureDesc) o;

        if (!function.equals(that.getFunction()))
            return false;

        return StringUtils.equals(dependentMeasureRef, that.getDependentMeasureRef());
    }

    @Override
    public String toString() {
        return "MeasureDesc [name=" + name + ", function=" + function + "]";
    }

}
