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
package org.apache.kylin.metadata.model.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;
import lombok.experimental.Delegate;

@Data
public class SchemaNode {

    @NonNull
    @Delegate
    SchemaNodeType type;

    @NonNull
    final String key;

    @Setter(value = AccessLevel.PRIVATE)
    Map<String, Object> attributes;

    @Setter(value = AccessLevel.PRIVATE)
    List<String> ignoreAttributes;

    @Setter(value = AccessLevel.PRIVATE)
    Map<String, Object> keyAttributes;

    private final int hashcode;

    public SchemaNode(SchemaNodeType type, String key) {
        this(type, key, Maps.newHashMap());
    }

    public SchemaNode(SchemaNodeType type, String key, Map<String, Object> attributes, String... ignore) {
        this.type = type;
        this.key = key;
        this.attributes = attributes;
        ignoreAttributes = Arrays.asList(ignore);
        keyAttributes = attributes.keySet().stream().filter(attribute -> !ignoreAttributes.contains(attribute))
                .collect(Collectors.toMap(Function.identity(), attributes::get));
        hashcode = Objects.hash(key);
    }

    /**
     * table columns node with identity as {DATABASE NAME}.{TABLE NAME}.{COLUMN NAME}
     * @param columnDesc
     * @return
     */
    public static SchemaNode ofTableColumn(ColumnDesc columnDesc) {
        return new SchemaNode(SchemaNodeType.TABLE_COLUMN,
                columnDesc.getTable().getIdentity() + "." + columnDesc.getName(),
                ImmutableMap.of("datatype", columnDesc.getDatatype()));
    }

    /**
     * table node with identity as {DATABASE NAME}.{TABLE NAME}
     * @param tableDesc
     * @return
     */
    public static SchemaNode ofTable(TableDesc tableDesc) {
        return new SchemaNode(SchemaNodeType.MODEL_TABLE, tableDesc.getIdentity());
    }

    /**
     * table node with identity as {DATABASE NAME}.{TABLE NAME}
     * @param tableRef
     * @return
     */
    public static SchemaNode ofTable(TableRef tableRef) {
        return new SchemaNode(SchemaNodeType.MODEL_TABLE, tableRef.getTableIdentity());
    }

    public static SchemaNode ofModelFactTable(TableRef tableRef, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_FACT, modelAlias + "/" + tableRef.getTableIdentity());
    }

    public static SchemaNode ofModelDimensionTable(TableRef tableRef, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_DIM, modelAlias + "/" + tableRef.getTableIdentity());
    }

    public static SchemaNode ofModelColumn(NDataModel.NamedColumn namedColumn, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_COLUMN, modelAlias + "/" + namedColumn.getName(),
                ImmutableMap.of("id", String.valueOf(namedColumn.getId())), "id");
    }

    public static SchemaNode ofModelCC(ComputedColumnDesc cc, String modelAlias, String factTable) {
        return new SchemaNode(SchemaNodeType.MODEL_CC, modelAlias + "/" + cc.getColumnName(),
                ImmutableMap.of("expression", transformCCExprToUpperCase(cc.getExpression()), "fact_table", factTable),
                "fact_table");
    }

    private static String transformCCExprToUpperCase(String expression) {
        String expr = expression.replace("\r\n", "\n");

        List<String> retainedStrings = new ArrayList<>(Collections.emptyList());
        Pattern patternQuote = Pattern.compile("('[\\S]+')");
        Matcher matcher = patternQuote.matcher(expr);
        int matchEnd = 0;
        int matchStart;
        while (matcher.find(matchEnd)) {
            retainedStrings.add(matcher.group(1));
            matchStart = matcher.start();
            expr = expr.substring(0, matchEnd).concat(StringUtils.upperCase(expr.substring(matchEnd, matchStart)))
                    .concat(expr.substring(matchStart));
            matchEnd = matcher.end();
        }
        expr = expr.substring(0, matchEnd).concat(StringUtils.upperCase(expr.substring(matchEnd)));

        if (retainedStrings.isEmpty()) {
            return StringUtils.upperCase(expr);
        }

        return expr;
    }

    public static SchemaNode ofDimension(NDataModel.NamedColumn namedColumn, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_DIMENSION, modelAlias + "/" + namedColumn.getName(),
                ImmutableMap.of("name", namedColumn.getName(), "alias_dot_column", namedColumn.getAliasDotColumn(),
                        "id", String.valueOf(namedColumn.getId())),
                "id");
    }

    public static SchemaNode ofMeasure(NDataModel.Measure measure, String modelAlias) {
        List<FunctionParameter> parameters = new ArrayList<>();
        if (measure.getFunction().getParameters() != null) {
            parameters = measure.getFunction().getParameters().stream()
                    .map(parameterDesc -> new FunctionParameter(parameterDesc.getType(), parameterDesc.getValue()))
                    .collect(Collectors.toList());
        }
        return new SchemaNode(SchemaNodeType.MODEL_MEASURE, modelAlias + "/" + measure.getName(),
                ImmutableMap.of("name", measure.getName(), "expression", measure.getFunction().getExpression(),
                        "returntype", measure.getFunction().getReturnType(), "parameters", parameters, "id",
                        String.valueOf(measure.getId())),
                "id");
    }

    public static SchemaNode ofPartition(PartitionDesc partitionDesc, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_PARTITION, modelAlias,
                ImmutableMap.of("column", partitionDesc.getPartitionDateColumn(), "format",
                        partitionDesc.getPartitionDateFormat() != null ? partitionDesc.getPartitionDateFormat() : ""));
    }

    public static SchemaNode ofMultiplePartition(MultiPartitionDesc multiPartitionDesc, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_MULTIPLE_PARTITION, modelAlias,
                ImmutableMap.of("columns", multiPartitionDesc.getColumns(), "partitions",
                        multiPartitionDesc.getPartitions().stream().map(MultiPartitionDesc.PartitionInfo::getValues)
                                .map(Arrays::asList).collect(Collectors.toList())));
    }

    // join type
    public static SchemaNode ofJoin(JoinTableDesc joinTableDesc, TableRef fkTableRef, TableRef pkTableRef,
            JoinDesc joinDesc, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_JOIN,
                modelAlias + "/" + fkTableRef.getAlias() + "-" + pkTableRef.getAlias(),
                ImmutableMap.<String, Object> builder()
                        .put("join_relation_type", joinTableDesc.getJoinRelationTypeEnum())
                        .put("primary_table", pkTableRef.getAlias()).put("foreign_table", fkTableRef.getAlias())
                        .put("join_type", joinDesc.getType())
                        .put("primary_keys", Arrays.asList(joinDesc.getPrimaryKey()))
                        .put("foreign_keys", Arrays.asList(joinDesc.getForeignKey()))
                        .put("non_equal_join_condition",
                                joinDesc.getNonEquiJoinCondition() != null
                                        ? joinDesc.getNonEquiJoinCondition().getExpr()
                                        : "")
                        .build());
    }

    public static SchemaNode ofFilter(String modelAlias, String condition) {
        return new SchemaNode(SchemaNodeType.MODEL_FILTER, modelAlias, ImmutableMap.of("condition", condition));
    }

    public static SchemaNode ofIndex(SchemaNodeType type, LayoutEntity layout, NDataModel model,
            Map<Integer, String> modelColumnMeasureIdMap, List<Integer> aggShardByColumns) {
        val colOrders = getLayoutIdColumn(layout, modelColumnMeasureIdMap);
        val shardBy = aggShardByColumns == null ? getLayoutShardByColumn(layout, modelColumnMeasureIdMap)
                : getColumnMeasureName(aggShardByColumns, modelColumnMeasureIdMap);
        val sortBy = getLayoutSortByColumn(layout, modelColumnMeasureIdMap);
        val key = model.getAlias() + "/" + String.join(",", colOrders) //
                + "/" + String.join(",", shardBy) //
                + "/" + String.join(",", sortBy);

        return new SchemaNode(type, key, ImmutableMap.of("col_orders", colOrders, "shard_by", shardBy, "sort_by",
                sortBy, "id", String.valueOf(layout.getId())), "id");
    }

    private static List<String> getLayoutIdColumn(LayoutEntity layout, Map<Integer, String> modelColumnMeasureIdMap) {
        return getColumnMeasureName(layout.getColOrder(), modelColumnMeasureIdMap);
    }

    private static List<String> getLayoutShardByColumn(LayoutEntity layout,
            Map<Integer, String> modelColumnMeasureIdMap) {
        return getColumnMeasureName(layout.getShardByColumns(), modelColumnMeasureIdMap);
    }

    private static List<String> getLayoutSortByColumn(LayoutEntity layout,
            Map<Integer, String> modelColumnMeasureIdMap) {
        return getColumnMeasureName(layout.getSortByColumns(), modelColumnMeasureIdMap);
    }

    private static List<String> getColumnMeasureName(List<Integer> columnIds,
            Map<Integer, String> modelColumnMeasureIdMap) {
        if (columnIds == null) {
            return Lists.newArrayList();
        }

        return columnIds.stream().map(modelColumnMeasureIdMap::get).collect(Collectors.toList());
    }

    public String getSubject() {
        return type.getSubject(key);
    }

    public String getDetail() {
        return type.getDetail(key, attributes);
    }

    public SchemaNodeIdentifier getIdentifier() {
        return new SchemaNodeIdentifier(this.getType(), this.getKey());
    }

    @Data
    @AllArgsConstructor
    public static class SchemaNodeIdentifier {
        private SchemaNodeType type;
        private String key;
    }

    @Data
    @AllArgsConstructor
    public static class FunctionParameter {
        private String type;
        private String value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SchemaNode that = (SchemaNode) o;
        return type == that.type && Objects.equals(key, that.key)
                && Objects.equals(this.keyAttributes, that.keyAttributes);
    }

    @Override
    public int hashCode() {
        //        return Objects.hash(type, key, this.keyAttributes);
        return hashcode;
    }
}
