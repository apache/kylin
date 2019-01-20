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

package org.apache.kylin.storage.parquet;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.ImmutableList;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class ParquetSchema implements NameMapping {
    public static final String CUBOID_ID = "CUBOID_ID";

    private final NameMapping mapping;
    private final List<TblColRef> dimensions;
    private final List<MeasureDesc> measures;
    private final Map<String, ColumnDataType> typeMap;

    public ParquetSchema(NameMapping mapping, List<TblColRef> dimensions, List<MeasureDesc> measures) {
        this.mapping = mapping;
        this.dimensions = ImmutableList.copyOf(dimensions);
        this.measures = ImmutableList.copyOf(measures);
        this.typeMap = genColumnTypes();
    }

    @Override
    public String getDimFieldName(TblColRef dim) {
        return mapping.getDimFieldName(dim);
    }

    @Override
    public String getMeasureFieldName(MeasureDesc measure) {
        return mapping.getMeasureFieldName(measure);
    }

    public List<TblColRef> getDimensions() {
        return dimensions;
    }

    public List<MeasureDesc> getMeasures() {
        return measures;
    }

    public int getTotalFieldCount() {
        return dimensions.size() + measures.size();
    }

    public ColumnDataType getTypeByName(String name) {
        return typeMap.get(name);
    }

    public MessageType buildMessageType(String schemaName) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(ParquetSchema.CUBOID_ID);

        for (TblColRef dim : dimensions) {
            buildField(dim.getType(), getDimFieldName(dim), builder);
        }

        for (MeasureDesc measure : measures) {
            buildField(measure.getFunction().getReturnDataType(), getMeasureFieldName(measure), builder);
        }

        return builder.named(schemaName);
    }

    private void buildField(DataType dataType, String fieldName, Types.MessageTypeBuilder builder) {
        if (dataType.isBoolean()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(fieldName);
        } else if (dataType.isInt() || dataType.isTinyInt() || dataType.isSmallInt()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(fieldName);
        } else if (dataType.isBigInt()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
        } else if (dataType.isDateTimeFamily()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
        } else if (dataType.isFloat()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(fieldName);
        } else if (dataType.isDouble()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(fieldName);
        } else if (dataType.isDecimal()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.DECIMAL).precision(dataType.getPrecision()).scale(dataType.getScale()).named(fieldName);
        } else if (dataType.isStringFamily()) {
            builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(fieldName);
        } else {
            builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(fieldName);
        }
    }

    private Map<String, ColumnDataType> genColumnTypes() {
        Map<String, ColumnDataType> map = Maps.newHashMap();

        for (TblColRef colRef : dimensions) {
            map.put(getDimFieldName(colRef), ColumnDataType.valueOf(colRef.getType()));
        }

        for (MeasureDesc measure : measures) {
            map.put(getMeasureFieldName(measure), ColumnDataType.valueOf(measure.getFunction().getReturnDataType()));
        }

        return map;
    }

    public enum ColumnDataType {
        DATATYPE_BOOLEAN, DATATYPE_INT, DATATYPE_LONG, DATATYPE_FLOAT, DATATYPE_DOUBLE, DATATYPE_DECIMAL, DATATYPE_STRING, DATATYPE_BINARY;

        public static ColumnDataType valueOf(DataType dataType) {
            if (dataType.isBoolean())
                return DATATYPE_BOOLEAN;
           else if (dataType.isInt() || dataType.isTinyInt() || dataType.isSmallInt())
               return DATATYPE_INT;
           else if (dataType.isBigInt())
               return DATATYPE_LONG;
           else if (dataType.isFloat())
               return DATATYPE_FLOAT;
           else if (dataType.isDouble())
               return DATATYPE_DOUBLE;
           else if (dataType.isDecimal())
               return DATATYPE_DECIMAL;
           else if (dataType.isDateTimeFamily())
               return DATATYPE_LONG;
           else if (dataType.isStringFamily())
               return DATATYPE_STRING;
           else
               return DATATYPE_BINARY;
        }
    }
}
