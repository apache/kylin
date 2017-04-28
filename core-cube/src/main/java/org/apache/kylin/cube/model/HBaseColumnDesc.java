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
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

import java.util.Arrays;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class HBaseColumnDesc implements java.io.Serializable {

    @JsonProperty("qualifier")
    private String qualifier;
    @JsonProperty("measure_refs")
    private String[] measureRefs;

    // these two will be assembled at runtime
    private MeasureDesc[] measures;
    private int[] measureIndex; // the index on CubeDesc.getMeasures()
    private String columnFamilyName;

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String[] getMeasureRefs() {
        return measureRefs;
    }

    public void setMeasureRefs(String[] measureRefs) {
        this.measureRefs = measureRefs;
    }

    public int[] getMeasureIndex() {
        return measureIndex;
    }

    public void setMeasureIndex(int[] index) {
        this.measureIndex = index;
    }

    public MeasureDesc[] getMeasures() {
        return measures;
    }

    public void setMeasures(MeasureDesc[] measures) {
        this.measures = measures;
    }

    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public void setColumnFamilyName(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }

    public int findMeasure(FunctionDesc function) {
        for (int i = 0; i < measures.length; i++) {
            if (measures[i].getFunction().equals(function)) {
                return i;
            }
        }
        return -1;
    }

    public boolean containsMeasure(String refName) {
        for (String ref : measureRefs) {
            if (ref.equals(refName))
                return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnFamilyName == null) ? 0 : columnFamilyName.hashCode());
        result = prime * result + ((qualifier == null) ? 0 : qualifier.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HBaseColumnDesc other = (HBaseColumnDesc) obj;
        if (columnFamilyName == null) {
            if (other.columnFamilyName != null)
                return false;
        } else if (!columnFamilyName.equals(other.columnFamilyName))
            return false;
        if (qualifier == null) {
            if (other.qualifier != null)
                return false;
        } else if (!qualifier.equals(other.qualifier))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "HBaseColumnDesc [qualifier=" + qualifier + ", measureRefs=" + Arrays.toString(measureRefs) + "]";
    }

}
