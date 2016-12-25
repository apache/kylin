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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ParameterDesc implements Serializable {

    public static ParameterDesc newInstance(Object... objs) {
        if (objs.length == 0)
            throw new IllegalArgumentException();
        
        ParameterDesc r = new ParameterDesc();
        
        Object obj = objs[0];
        if (obj instanceof TblColRef) {
            TblColRef col = (TblColRef) obj;
            r.type = FunctionDesc.PARAMETER_TYPE_COLUMN;
            r.value = col.getIdentity();
            r.colRefs = ImmutableList.of(col);
        } else {
            r.type = FunctionDesc.PARAMETER_TYPE_CONSTANT;
            r.value = (String) obj;
        }
        
        if (objs.length >= 2) {
            r.nextParameter = newInstance(Arrays.copyOfRange(objs, 1, objs.length));
            if (r.nextParameter.colRefs.size() > 0) {
                if (r.colRefs.isEmpty())
                    r.colRefs = r.nextParameter.colRefs;
                else
                    r.colRefs = ImmutableList.copyOf(Iterables.concat(r.colRefs, r.nextParameter.colRefs));
            }
        }
        return r;
    }
    
    @JsonProperty("type")
    private String type;
    @JsonProperty("value")
    private String value;

    @JsonProperty("next_parameter")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private ParameterDesc nextParameter;

    private List<TblColRef> colRefs = ImmutableList.of();

    public String getType() {
        return type;
    }

    public byte[] getBytes() throws UnsupportedEncodingException {
        return value.getBytes("UTF-8");
    }

    public String getValue() {
        return value;
    }
    
    void setValue(String value) {
        this.value = value;
    }

    public List<TblColRef> getColRefs() {
        return colRefs;
    }
    
    void setColRefs(List<TblColRef> colRefs) {
        this.colRefs = colRefs;
    }

    public ParameterDesc getNextParameter() {
        return nextParameter;
    }

    public boolean isColumnType() {
        return FunctionDesc.PARAMETER_TYPE_COLUMN.equals(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ParameterDesc that = (ParameterDesc) o;

        if (type != null ? !type.equals(that.type) : that.type != null)
            return false;
        
        ParameterDesc p = this, q = that;
        int refi = 0, refj = 0;
        for (; p != null && q != null; p = p.nextParameter, q = q.nextParameter) {
            if (p.isColumnType()) {
                if (q.isColumnType() == false)
                    return false;
                if (refi >= this.colRefs.size() || refj >= that.colRefs.size())
                    return false;
                if (this.colRefs.get(refi).equals(that.colRefs.get(refj)) == false)
                    return false;
                refi++;
                refj++;
            } else {
                if (q.isColumnType() == true)
                    return false;
                if (p.value.equals(q.value) == false)
                    return false;
            }
        }
        
        return p == null && q == null;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (colRefs != null ? colRefs.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ParameterDesc [type=" + type + ", value=" + value + ", nextParam=" + nextParameter + "]";
    }

}
