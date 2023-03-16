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

package org.apache.kylin.metadata.acl;

import java.util.Locale;
import java.util.Set;

import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SensitiveDataMask {

    private static final Set<String> VALID_DATA_TYPES = Sets.newHashSet(DataType.STRING, DataType.VARCHAR,
            DataType.CHAR, DataType.INT, DataType.INTEGER, DataType.BIGINT, DataType.SMALL_INT, DataType.TINY_INT,
            DataType.DOUBLE, DataType.FLOAT, DataType.REAL, DataType.DECIMAL, DataType.DATE, DataType.TIMESTAMP,
            DataType.DATETIME);

    public static boolean isValidDataType(String dataType) {
        int parenthesesIdx = dataType.indexOf('(');
        return VALID_DATA_TYPES
                .contains(parenthesesIdx > -1 ? dataType.substring(0, parenthesesIdx).trim().toLowerCase(Locale.ROOT)
                        : dataType.trim().toLowerCase(Locale.ROOT));
    }

    public enum MaskType {
        DEFAULT(0), // mask sensitive data by type with default values
        AS_NULL(1); // mask all sensitive data as NULL

        int priority = 0; // smaller number stands for higher priority

        MaskType(int priority) {
            this.priority = priority;
        }

        public MaskType merge(MaskType other) {
            if (other == null) {
                return this;
            }
            return this.priority < other.priority ? this : other;
        }
    }

    @JsonProperty
    String column;

    @JsonProperty
    MaskType type;

    public SensitiveDataMask() {
    }

    public SensitiveDataMask(String column, MaskType type) {
        this.column = column;
        this.type = type;
    }

    public MaskType getType() {
        return type;
    }

    public String getColumn() {
        return column;
    }
}
