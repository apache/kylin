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
package org.apache.kylin.metadata.query;

import lombok.NoArgsConstructor;

public class StructField {
    private String name;
    private int dataType;
    private String dataTypeName;
    private int precision;
    private int scale;
    private boolean nullable;

    public StructField(String name, int dataType, String dataTypeName, int precision, int scale, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.dataTypeName = dataTypeName;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public int getDataType() {
        return dataType;
    }

    public String getDataTypeName() {
        return dataTypeName;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    @NoArgsConstructor
    static public class StructFieldBuilder {
        private String name;
        private int dataType;
        private String dataTypeName;
        private int precision;
        private int scale;
        private boolean nullable;

        public StructFieldBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public StructFieldBuilder setDataType(int dataType) {
            this.dataType = dataType;
            return this;
        }

        public StructFieldBuilder setDataTypeName(String dataTypeName) {
            this.dataTypeName = dataTypeName;
            return this;
        }

        public StructFieldBuilder setPrecision(int precision) {
            this.precision = precision;
            return this;
        }

        public StructFieldBuilder setScale(int scale) {
            this.scale = scale;
            return this;
        }

        public StructFieldBuilder setNullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public StructField createStructField() {
            return new StructField(name, dataType, dataTypeName, precision, scale, nullable);
        }
    }

}
