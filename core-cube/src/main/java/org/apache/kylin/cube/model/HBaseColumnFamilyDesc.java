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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.metadata.model.MeasureDesc;

import java.util.Arrays;
import java.util.Objects;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class HBaseColumnFamilyDesc implements java.io.Serializable {

    @JsonProperty("name")
    private String name;
    @JsonProperty("columns")
    private HBaseColumnDesc[] columns;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public HBaseColumnDesc[] getColumns() {
        return columns;
    }

    public void setColumns(HBaseColumnDesc[] columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "HBaseColumnFamilyDesc [name=" + name + ", columns=" + Arrays.toString(columns) + "]";
    }

    public boolean isMemoryHungry() {
        for (HBaseColumnDesc hBaseColumnDesc : columns) {
            for (MeasureDesc measureDesc : hBaseColumnDesc.getMeasures()) {
                if (measureDesc.getFunction().getMeasureType().isMemoryHungry()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HBaseColumnFamilyDesc that = (HBaseColumnFamilyDesc) o;
        return Objects.equals(name, that.name) &&
                Arrays.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name);
        result = 31 * result + Arrays.hashCode(columns);
        return result;
    }
}
