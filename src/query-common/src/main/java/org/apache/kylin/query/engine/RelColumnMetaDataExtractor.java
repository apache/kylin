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

package org.apache.kylin.query.engine;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kylin.metadata.query.StructField;

/**
 * extract table and column metadata from context
 */
public class RelColumnMetaDataExtractor {
    private RelColumnMetaDataExtractor() {
    }

    public static List<StructField> getColumnMetadata(RelDataType rowType) {
        return rowType.getFieldList().stream().map(type -> relDataTypeToStructField(type.getKey(), type.getValue()))
                .collect(Collectors.toList());
    }

    public static List<StructField> getColumnMetadata(RelNode rel) {
        return getColumnMetadata(rel.getRowType());
    }

    private static StructField relDataTypeToStructField(String fieldName, RelDataType relDataType) {
        return new StructField(fieldName, relDataType.getSqlTypeName().getJdbcOrdinal(),
                relDataType.getSqlTypeName().getName(), getPrecision(relDataType), getScale(relDataType),
                relDataType.isNullable());
    }

    private static int getScale(RelDataType type) {
        return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED ? 0 : type.getScale();
    }

    private static int getPrecision(RelDataType type) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED ? 0 : type.getPrecision();
    }

}
