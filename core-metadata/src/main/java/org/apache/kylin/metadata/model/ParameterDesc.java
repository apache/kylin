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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

/**
 */
@SuppressWarnings("serial")
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
            r.colRef = col;
        } else {
            r.type = FunctionDesc.PARAMETER_TYPE_CONSTANT;
            r.value = (String) obj;
        }

        if (objs.length >= 2) {
            r.nextParameter = newInstance(Arrays.copyOfRange(objs, 1, objs.length));
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

    private TblColRef colRef = null;
    private List<TblColRef> allColRefsIncludingNexts = null;
    private Set<PlainParameter> plainParameters = null;

    // Lazy evaluation
    public Set<PlainParameter> getPlainParameters() {
        if (plainParameters == null) {
            plainParameters = PlainParameter.createFromParameterDesc(this);
        }
        return plainParameters;
    }

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
    
    public TblColRef getColRef() {
        return colRef;
    }

    void setColRef(TblColRef colRef) {
        this.colRef = colRef;
    }

    public List<TblColRef> getColRefs() {
        if (allColRefsIncludingNexts == null) {
            List<TblColRef> all = new ArrayList<>(2);
            ParameterDesc p = this;
            while (p != null) {
                if (p.isColumnType())
                    all.add(p.getColRef());
                
                p = p.nextParameter;
            }
            allColRefsIncludingNexts = all;
        }
        return allColRefsIncludingNexts;
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
        for (; p != null && q != null; p = p.nextParameter, q = q.nextParameter) {
            if (p.isColumnType()) {
                if (q.isColumnType() == false)
                    return false;
                if (this.getColRef().equals(that.getColRef()) == false)
                    return false;
            } else {
                if (q.isColumnType() == true)
                    return false;
                if (p.value.equals(q.value) == false)
                    return false;
            }
        }

        return p == null && q == null;
    }

    public boolean equalInArbitraryOrder(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ParameterDesc that = (ParameterDesc) o;

        Set<PlainParameter> thisPlainParams = this.getPlainParameters();
        Set<PlainParameter> thatPlainParams = that.getPlainParameters();

        return thisPlainParams.containsAll(thatPlainParams) && thatPlainParams.containsAll(thisPlainParams);
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (colRef != null ? colRef.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        String thisStr = isColumnType() ? colRef.toString() : value;
        return nextParameter == null ? thisStr : thisStr + "," + nextParameter.toString();
    }

    /**
     * PlainParameter is created to present ParameterDesc in List style.
     * Compared to ParameterDesc its advantage is:
     * 1. easy to compare without considering order
     * 2. easy to compare one by one
     */
    private static class PlainParameter {
        private String type;
        private String value;
        private TblColRef colRef = null;

        private PlainParameter() {
        }

        public boolean isColumnType() {
            return FunctionDesc.PARAMETER_TYPE_COLUMN.equals(type);
        }

        static Set<PlainParameter> createFromParameterDesc(ParameterDesc parameterDesc) {
            Set<PlainParameter> result = Sets.newHashSet();
            ParameterDesc local = parameterDesc;
            while (local != null) {
                if (local.isColumnType()) {
                    result.add(createSingleColumnParameter(local));
                } else {
                    result.add(createSingleValueParameter(local));
                }
                local = local.nextParameter;
            }
            return result;
        }

        static PlainParameter createSingleValueParameter(ParameterDesc parameterDesc) {
            PlainParameter single = new PlainParameter();
            single.type = parameterDesc.type;
            single.value = parameterDesc.value;
            return single;
        }

        static PlainParameter createSingleColumnParameter(ParameterDesc parameterDesc) {
            PlainParameter single = new PlainParameter();
            single.type = parameterDesc.type;
            single.value = parameterDesc.value;
            single.colRef = parameterDesc.colRef;
            return single;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (colRef != null ? colRef.hashCode() : 0);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PlainParameter that = (PlainParameter) o;

            if (type != null ? !type.equals(that.type) : that.type != null)
                return false;

            if (this.isColumnType()) {
                if (!that.isColumnType())
                    return false;
                if (!this.colRef.equals(that.colRef)) {
                    return false;
                }
            } else {
                if (that.isColumnType())
                    return false;
                if (!this.value.equals(that.value))
                    return false;
            }

            return true;
        }
    }
}
