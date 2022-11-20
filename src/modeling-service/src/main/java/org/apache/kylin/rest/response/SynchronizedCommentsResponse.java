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
package org.apache.kylin.rest.response;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.request.ModelRequest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class SynchronizedCommentsResponse {

    @JsonProperty("model_request")
    private ModelRequest modelRequest;

    @JsonProperty("conflict_info")
    private ConflictInfo conflictInfo;

    @Data
    @AllArgsConstructor
    public static class ConflictInfo implements Serializable {
        @JsonProperty("cols_with_same_comment")
        private List<String> colsWithSameComment;
        @JsonProperty("dims_origin_from_same_col")
        private List<String> dimsOriginFromSameCol;
    }

    public static final Pattern SPECIAL_CHAR_PTN = Pattern.compile("((?![\\u4E00-\\u9FA5a-zA-Z0-9 _\\-()%?（）]+).)*");

    public void syncComment(ModelRequest modelRequest) {
        this.modelRequest = modelRequest;
        Map<String, ColumnDesc> columnDescMap = getColumnDescMap();
        syncDimensionNames(columnDescMap, modelRequest.getSimplifiedDimensions());
        syncMeasureComments(columnDescMap, modelRequest.getSimplifiedMeasures());
    }

    private void syncDimensionNames(Map<String, ColumnDesc> columnDescMap,
            List<NDataModel.NamedColumn> simplifiedDimensions) {
        List<String> colsWithSameComment = Lists.newArrayList();
        List<String> dimsOriginFromSameCol = Lists.newArrayList();
        simplifiedDimensions.forEach(namedColumn -> {
            String aliasDotColumn = StringUtils.upperCase(namedColumn.getAliasDotColumn());
            ColumnDesc columnDesc = columnDescMap.get(aliasDotColumn);
            String name = StringUtils.upperCase(namedColumn.getColTableName());
            if (columnDesc == null) {
                return;
            }
            String canonicalName = StringUtils.upperCase(columnDesc.getCanonicalName());
            List<ColumnDesc> columnDescList = simplifiedDimensions.stream()
                    .map(simpleDimension -> columnDescMap
                            .get(StringUtils.upperCase(simpleDimension.getAliasDotColumn())))
                    .filter(Objects::nonNull)
                    .filter(column -> StringUtils.upperCase(column.getCanonicalName()).equals(canonicalName))
                    .collect(Collectors.toList());
            long dimsOriginFromSameColCount = columnDescList.size();
            long colsWithSameCommentCount = simplifiedDimensions.stream()
                    .map(simpleDimension -> columnDescMap
                            .get(StringUtils.upperCase(simpleDimension.getAliasDotColumn())))
                    .filter(Objects::nonNull)
                    .filter(column -> StringUtils.isNotBlank(column.getComment())
                            && StringUtils.isNotBlank(columnDesc.getComment())
                            && StringUtils.upperCase(column.getComment())
                                    .equals(StringUtils.upperCase(columnDesc.getComment())))
                    .count();
            if (dimsOriginFromSameColCount > 1) {
                dimsOriginFromSameCol.add(name);
            }
            if (colsWithSameCommentCount > 1) {
                colsWithSameComment.add(name);
            }
            if (dimsOriginFromSameColCount == 1 && colsWithSameCommentCount == 1
                    && StringUtils.isNotBlank(columnDesc.getComment())) {
                name = columnDesc.getComment();
            }
            namedColumn.setName(name);
        });
        this.modelRequest.setSimplifiedDimensions(simplifiedDimensions);
        this.conflictInfo = new ConflictInfo(colsWithSameComment, dimsOriginFromSameCol);
    }

    private void syncMeasureComments(Map<String, ColumnDesc> columnDescMap,
            List<SimplifiedMeasure> simplifiedMeasures) {
        simplifiedMeasures.forEach(simplifiedMeasure -> {
            List<ParameterResponse> parameterResponses = simplifiedMeasure.getParameterValue().stream().filter(
                    parameterResponse -> !FunctionDesc.PARAMETER_TYPE_CONSTANT.equals(parameterResponse.getType()))
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(parameterResponses)) {
                return;
            }
            String aliasDotColumn = parameterResponses.get(0).getValue();
            ColumnDesc columnDesc = columnDescMap.get(aliasDotColumn);
            if (columnDesc != null && StringUtils.isNotBlank(columnDesc.getComment())) {
                simplifiedMeasure.setComment(columnDesc.getComment());
            }
        });
        modelRequest.setSimplifiedMeasures(simplifiedMeasures);
    }

    public Map<String, ColumnDesc> getColumnDescMap() {
        Map<String, ColumnDesc> columnDescMap = Maps.newHashMap();
        String project = modelRequest.getProject();
        String factTableName = modelRequest.getRootFactTableName();
        Set<String> computedColumnNames = modelRequest.getComputedColumnNames().stream().map(StringUtils::upperCase)
                .collect(Collectors.toSet());
        List<JoinTableDesc> joinTables = modelRequest.getJoinTables();
        List<NDataModel.NamedColumn> simplifiedDimensions = modelRequest.getSimplifiedDimensions();
        simplifiedDimensions.forEach(namedColumn -> putColumnDesc(project, computedColumnNames, factTableName,
                joinTables, columnDescMap, namedColumn.getAliasDotColumn()));
        List<SimplifiedMeasure> simplifiedMeasures = modelRequest.getSimplifiedMeasures();
        simplifiedMeasures.forEach(simplifiedMeasure -> {
            List<ParameterResponse> parameterResponses = simplifiedMeasure.getParameterValue().stream().filter(
                    parameterResponse -> !FunctionDesc.PARAMETER_TYPE_CONSTANT.equals(parameterResponse.getType()))
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(parameterResponses)) {
                return;
            }
            parameterResponses.forEach(parameterResponse -> putColumnDesc(project, computedColumnNames, factTableName,
                    joinTables, columnDescMap, parameterResponse.getValue()));
        });
        return columnDescMap;
    }

    private void putColumnDesc(String project, Set<String> computedColumnNames, String factTableName,
            List<JoinTableDesc> joinTables, Map<String, ColumnDesc> columnDescMap, String aliasDotColumn) {
        if (aliasDotColumn == null) {
            return;
        }
        ColumnDesc columnDesc = getColumnDescByAliasDotColumn(project, StringUtils.upperCase(aliasDotColumn),
                computedColumnNames, StringUtils.upperCase(factTableName), joinTables);
        if (columnDesc != null) {
            columnDescMap.put(aliasDotColumn, columnDesc);
        }
    }

    private ColumnDesc getColumnDescByAliasDotColumn(String project, String aliasDotColumn,
            Set<String> computedColumnNames, String factTableName, List<JoinTableDesc> joinTables) {
        String[] aliasDotColumnSplit = aliasDotColumn.split("\\.");
        if (aliasDotColumnSplit.length != 2) {
            return null;
        }
        String tableName = aliasDotColumnSplit[0];
        String columnName = aliasDotColumnSplit[1];
        if (computedColumnNames.contains(aliasDotColumn) || computedColumnNames.contains(columnName)) {
            return null;
        }
        String factTableAlias = factTableName;
        if (factTableName.contains(".") && factTableName.split("\\.").length == 2) {
            factTableAlias = factTableName.split("\\.")[1];
        }
        NTableMetadataManager mgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        TableDesc tableDesc;
        if (tableName.equals(factTableAlias)) {
            tableDesc = mgr.getTableDesc(factTableName);
        } else {
            Optional<JoinTableDesc> joinTable = joinTables.stream()
                    .filter(joinTableDesc -> StringUtils.upperCase(joinTableDesc.getAlias()).equals(tableName))
                    .findFirst();
            if (joinTable.isPresent()) {
                tableDesc = mgr.getTableDesc(joinTable.get().getTable());
            } else {
                return null;
            }
        }
        Optional<ColumnDesc> column = Arrays.stream(tableDesc.getColumns())
                .filter(columnDesc -> StringUtils.upperCase(columnDesc.getName()).equals(columnName)).findFirst();
        if (column.isPresent()) {
            ColumnDesc columnDesc = column.get();
            String comment = columnDesc.getComment();
            if (comment != null) {
                columnDesc.setComment(replaceInvalidCharacters(comment.trim()));
            }
            return columnDesc;
        }
        return null;
    }

    private String replaceInvalidCharacters(String name) {
        Matcher matcher = SPECIAL_CHAR_PTN.matcher(name);
        return matcher.replaceAll("");
    }
}
