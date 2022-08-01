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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataType implements Serializable {
    @Getter
    @Setter
    @JsonProperty("type_name")
    private SqlTypeName typeName;

    @Getter
    @Setter
    @JsonProperty("precision")
    private int precision = -1;

    @Getter
    @Setter
    @JsonProperty("scale")
    private int scale = -1;

    public DataType() {
    }

    public DataType(RelDataType relDataType) {
        this.typeName = relDataType.getSqlTypeName();
        this.precision = relDataType.getPrecision();
        this.scale = relDataType.getScale();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof DataType)) {
            return false;
        }
        DataType other = (DataType) obj;
        return typeName == other.getTypeName(); // ignore precision/scala
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + typeName.hashCode();
        return result;
    }
}
