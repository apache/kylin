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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created with IntelliJ IDEA. User: lukhan Date: 9/24/13 Time: 10:41 AM To
 * change this template use File | Settings | File Templates.
 */

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class MeasureDesc {

    @JsonProperty("id")
    private int id;
    @JsonProperty("name")
    private String name;
    @JsonProperty("function")
    private FunctionDesc function;
    @JsonProperty("dependent_measure_ref")
    private String dependentMeasureRef;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

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

    public String getDependentMeasureRef() {
        return dependentMeasureRef;
    }

    public void setDependentMeasureRef(String dependentMeasureRef) {
        this.dependentMeasureRef = dependentMeasureRef;
    }

    public boolean isHolisticCountDistinct() {
        return function.isHolisticCountDistinct();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        MeasureDesc that = (MeasureDesc) o;

        if (id != that.id)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return "MeasureDesc [name=" + name + ", function=" + function + "]";
    }

}
