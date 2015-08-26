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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ParameterDesc {

    public static final String COLUMN_TYPE = "column";

    @JsonProperty("type")
    private String type;
    @JsonProperty("value")
    private String value;
    
    @JsonProperty("counter")
    private String counter;

    private List<TblColRef> colRefs;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public byte[] getBytes() throws UnsupportedEncodingException {
        return value.getBytes("UTF-8");
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getCounter() {
        return counter;
    }

    public void setCounter(String counter) {
        this.counter = counter;
    }

    public List<TblColRef> getColRefs() {
        return colRefs;
    }

    public void setColRefs(List<TblColRef> colRefs) {
        this.colRefs = colRefs;
    }

    public boolean isColumnType() {
        return COLUMN_TYPE.equals(type);
    }

    public void normalizeColumnValue() {
        if (isColumnType()) {
            String values[] = value.split("\\s*,\\s*");
            for (int i = 0; i < values.length; i++)
                values[i] = values[i].toUpperCase();
            Arrays.sort(values);
            value = StringUtils.join(values, ",");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParameterDesc that = (ParameterDesc) o;

        if (counter != null ? !counter.equals(that.counter) : that.counter != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (counter != null ? counter.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ParameterDesc [type=" + type + ", value=" + value + ", counter=" + counter + "]";
    }

}
