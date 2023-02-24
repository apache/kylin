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

import static org.apache.kylin.metadata.model.NDataModel.ColumnStatus.DIMENSION;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.HashCodeExclude;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.acl.NDataModelAclParams;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ColExcludedChecker;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.util.scd2.SimplifiedJoinTableDesc;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;
import org.apache.kylin.rest.util.ModelUtils;
import org.apache.kylin.rest.util.SCD2SimplificationConvertUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.Lists;

import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
public class NDataModelResponse extends NDataModel {

    @JsonProperty("status")
    private ModelStatusToDisplayEnum status;

    @JsonProperty("last_build_end")
    private String lastBuildEnd;

    @JsonProperty("storage")
    private long storage;

    @JsonProperty("source")
    private long source;

    @JsonProperty("expansion_rate")
    private String expansionrate;

    @JsonProperty("usage")
    private long usage;

    @JsonProperty("root_fact_table_deleted")
    private boolean rootFactTableDeleted = false;

    @JsonProperty("segments")
    private List<NDataSegmentResponse> segments = new ArrayList<>();

    @JsonProperty("available_indexes_count")
    private long availableIndexesCount;

    @JsonProperty("empty_indexes_count")
    private long emptyIndexesCount;

    @JsonProperty("segment_holes")
    private List<SegmentRange> segmentHoles;

    @JsonProperty("inconsistent_segment_count")
    private long inconsistentSegmentCount;

    @JsonProperty("total_indexes")
    private long totalIndexes;

    @JsonProperty("forbidden_online")
    private boolean forbiddenOnline = false;

    @JsonProperty("join_tables")
    private List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs;

    @JsonProperty("last_build_time")
    private long lastBuildTime;

    @JsonProperty("has_base_table_index")
    private boolean hasBaseTableIndex;

    @JsonProperty("has_base_agg_index")
    private boolean hasBaseAggIndex;

    @JsonProperty("has_segments")
    private boolean hasSegments;

    @JsonProperty("second_storage_size")
    private long secondStorageSize;

    @JsonProperty("second_storage_nodes")
    private Map<String, List<SecondStorageNode>> secondStorageNodes;

    @JsonProperty("second_storage_enabled")
    private boolean secondStorageEnabled;

    @JsonProperty("model_update_enabled")
    private boolean modelUpdateEnabled = true;

    private long lastModify;

    @JsonIgnore
    private List<SimplifiedNamedColumn> simplifiedDims;

    @Getter(lazy = true)
    @JsonIgnore
    private final NDataModel lazyModel = originModel();

    public NDataModelResponse() {
        super();
    }

    @Override
    public KylinConfig getConfig() {
        return super.getConfig() == null ? KylinConfig.getInstanceFromEnv() : super.getConfig();
    }

    public NDataModelResponse(NDataModel dataModel) {
        super(dataModel);
        this.setConfig(dataModel.getConfig());
        this.setProject(dataModel.getProject());
        this.setMvcc(dataModel.getMvcc());
        this.setModelType(dataModel.getModelType());
        this.lastModify = lastModified;
        this.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(dataModel.getJoinTables()));

        // filter out and hide internal measures from users
        this.setAllMeasures(getAllMeasures().stream().filter(m -> m.getType() != MeasureType.INTERNAL)
                .collect(Collectors.toList()));
    }

    @JsonIgnore
    public boolean isEmptyModel() {
        List<SimplifiedMeasure> simplifiedMeasures = getSimplifiedMeasures();
        return getNamedColumns().isEmpty() && simplifiedMeasures.size() == 1
                && "COUNT_ALL".equals(simplifiedMeasures.get(0).getName());
    }

    @JsonIgnore
    public boolean isPartitionColumnInDims() {
        PartitionDesc partitionDesc = getPartitionDesc();
        if (partitionDesc == null || partitionDesc.getPartitionDateColumn() == null) {
            return false;
        }
        String partitionColumn = partitionDesc.getPartitionDateColumn();
        return getNamedColumns().stream().anyMatch(dim -> dim.getAliasDotColumn().equalsIgnoreCase(partitionColumn));
    }

    @JsonProperty("simplified_dimensions")
    public List<SimplifiedNamedColumn> getNamedColumns() {
        if (simplifiedDims != null) {
            return simplifiedDims;
        }
        fillDimensions(true);
        return simplifiedDims;
    }

    public void enrichDerivedDimension() {
        fillDimensions(false);
    }

    private void fillDimensions(boolean onlyNormalDim) {
        NTableMetadataManager tableMetadata = null;
        if (!isBroken()) {
            tableMetadata = NTableMetadataManager.getInstance(getConfig(), this.getProject());
        }

        ColExcludedChecker excludedChecker = new ColExcludedChecker(getConfig(), getProject(), this);
        List<SimplifiedNamedColumn> dimList = Lists.newArrayList();
        for (NamedColumn col : getAllNamedColumns()) {
            if (col.isDimension()) {
                dimList.add(transColumnToDim(excludedChecker, col, tableMetadata));
            }
        }
        if (!onlyNormalDim) {
            List<NamedColumn> allNameColCopy = Lists.newArrayList();
            for (NamedColumn col : getAllNamedColumns()) {
                allNameColCopy.add(NamedColumn.copy(col));
            }

            Map<String, NamedColumn> columnMap = allNameColCopy.stream().filter(NamedColumn::isExist)
                    .collect(Collectors.toMap(NamedColumn::getAliasDotColumn, Function.identity()));
            for (JoinTableDesc joinTable : getJoinTables()) {
                if (!joinTable.isFlattenable() && isFkAllDim(joinTable.getJoin().getForeignKey(), columnMap)) {
                    for (TblColRef col : joinTable.getTableRef().getColumns()) {
                        NamedColumn namedColumn = columnMap.get(col.getAliasDotName());
                        if (!namedColumn.isDimension()) {
                            dimList.add(transColumnToDim(excludedChecker, namedColumn, tableMetadata));
                            namedColumn.setStatus(DIMENSION);
                        }
                    }
                }
            }
            setAllNamedColumns(allNameColCopy);
        }

        simplifiedDims = dimList;
    }

    private boolean isFkAllDim(String[] foreignKeys, Map<String, NamedColumn> columnMap) {
        if (foreignKeys == null) {
            return false;
        }
        for (String fkCol : foreignKeys) {
            if (!columnMap.get(fkCol).isDimension()) {
                return false;
            }
        }
        return true;
    }

    private NDataModel originModel() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataModelDesc(this.getUuid());
    }

    public SimplifiedNamedColumn transColumnToDim(ColExcludedChecker excludedChecker, NamedColumn col,
            NTableMetadataManager tableMetadata) {
        SimplifiedNamedColumn simplifiedDimension = new SimplifiedNamedColumn(col);
        simplifiedDimension.setStatus(DIMENSION);
        TblColRef colRef = findColumnByAlias(simplifiedDimension.getAliasDotColumn());
        if (colRef == null || tableMetadata == null) {
            return simplifiedDimension;
        }
        if (excludedChecker.isExcludedCol(colRef)
                && !colRef.getTableRef().getTableIdentity().equals(getLazyModel().getRootFactTableName())) {
            simplifiedDimension.setExcluded(true);
        }
        TableExtDesc tableExt = tableMetadata.getTableExtIfExists(colRef.getTableRef().getTableDesc());
        if (tableExt != null) {
            TableExtDesc.ColumnStats columnStats = tableExt.getColumnStatsByName(colRef.getName());
            if (colRef.getColumnDesc().getComment() != null) {
                simplifiedDimension.setComment(colRef.getColumnDesc().getComment());
            }
            if (colRef.getColumnDesc().getType() != null) {
                simplifiedDimension.setType(colRef.getColumnDesc().getType().toString());
            }
            if (columnStats != null) {
                simplifiedDimension.setCardinality(columnStats.getCardinality());
                simplifiedDimension.setMaxValue(columnStats.getMaxValue());
                simplifiedDimension.setMinValue(columnStats.getMinValue());
                simplifiedDimension.setMaxLengthValue(columnStats.getMaxLengthValue());
                simplifiedDimension.setMinLengthValue(columnStats.getMinLengthValue());

                ArrayList<String> simple = Lists.newArrayList();
                tableExt.getSampleRows()
                        .forEach(row -> simple.add(row[tableExt.getAllColumnStats().indexOf(columnStats)]));
                simplifiedDimension.setSimple(simple);
            }
        }
        return simplifiedDimension;
    }

    @JsonProperty("all_measures")
    public List<Measure> getMeasures() {
        return getAllMeasures().stream().filter(m -> !m.isTomb()).collect(Collectors.toList());
    }

    @JsonProperty("model_broken")
    public boolean isModelBroken() {
        return this.isBroken();
    }

    @JsonProperty("simplified_tables")
    public List<SimplifiedTableResponse> getSimpleTables() {
        List<SimplifiedTableResponse> simpleTables = new ArrayList<>();
        for (TableRef tableRef : getAllTables()) {
            SimplifiedTableResponse simpleTable = new SimplifiedTableResponse();
            simpleTable.setTable(tableRef.getTableIdentity());
            List<SimplifiedColumnResponse> columns = getSimplifiedColumns(tableRef);
            simpleTable.setColumns(columns);
            simpleTables.add(simpleTable);
        }
        return simpleTables;
    }

    @JsonProperty("simplified_measures")
    public List<SimplifiedMeasure> getSimplifiedMeasures() {
        List<NDataModel.Measure> measures = getAllMeasures();
        List<SimplifiedMeasure> measureResponses = new ArrayList<>();
        for (NDataModel.Measure measure : measures) {
            if (measure.isTomb()) {
                continue;
            }
            measureResponses.add(SimplifiedMeasure.fromMeasure(measure));
        }
        return measureResponses;
    }

    private List<SimplifiedColumnResponse> getSimplifiedColumns(TableRef tableRef) {
        List<SimplifiedColumnResponse> columns = new ArrayList<>();
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getConfig(), getProject());
        for (ColumnDesc columnDesc : tableRef.getTableDesc().getColumns()) {
            TableExtDesc tableExtDesc = tableMetadataManager.getOrCreateTableExt(tableRef.getTableDesc());
            SimplifiedColumnResponse simplifiedColumnResponse = new SimplifiedColumnResponse();
            simplifiedColumnResponse.setName(columnDesc.getName());
            simplifiedColumnResponse.setComment(columnDesc.getComment());
            simplifiedColumnResponse.setDataType(columnDesc.getDatatype());
            simplifiedColumnResponse.setComputedColumn(columnDesc.isComputedColumn());
            // get column cardinality
            final TableExtDesc.ColumnStats columnStats = tableExtDesc.getColumnStatsByName(columnDesc.getName());
            if (columnStats != null) {
                simplifiedColumnResponse.setCardinality(columnStats.getCardinality());
            }

            columns.add(simplifiedColumnResponse);
        }
        return columns;
    }

    @Data
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @EqualsAndHashCode
    @ToString
    public static class SimplifiedNamedColumn extends NamedColumn implements Serializable {

        public SimplifiedNamedColumn(NamedColumn namedColumn) {
            this.id = namedColumn.getId();
            this.aliasDotColumn = namedColumn.getAliasDotColumn();
            this.status = namedColumn.getStatus();
            this.name = namedColumn.getName();
        }

        @HashCodeExclude
        @JsonProperty("excluded")
        private boolean excluded;

        @JsonProperty("cardinality")
        private Long cardinality;

        @JsonProperty("min_value")
        private String minValue;

        @JsonProperty("max_value")
        private String maxValue;

        @JsonProperty("max_length_value")
        private String maxLengthValue;

        @JsonProperty("min_length_value")
        private String minLengthValue;

        @JsonProperty("null_count")
        private Long nullCount;

        @JsonProperty("comment")
        private String comment;

        @JsonProperty("type")
        private String type;

        @JsonProperty("simple")
        private ArrayList<String> simple;
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelOldParams oldParams;

    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelAclParams aclParams;

    @JsonGetter("selected_columns")
    public List<SimplifiedNamedColumn> getSelectedColumns() {
        List<SimplifiedNamedColumn> selectedColumns = Lists.newArrayList();
        NTableMetadataManager tableMetadata = null;
        KylinConfig config = getConfig();
        if (!isBroken()) {
            tableMetadata = NTableMetadataManager.getInstance(config, this.getProject());
        }
        ColExcludedChecker excludedChecker = new ColExcludedChecker(config, getProject(), this);
        for (NamedColumn namedColumn : getAllSelectedColumns()) {
            SimplifiedNamedColumn simplifiedNamedColumn = new SimplifiedNamedColumn(namedColumn);
            TblColRef colRef = findColumnByAlias(simplifiedNamedColumn.getAliasDotColumn());
            if (simplifiedNamedColumn.getStatus() == DIMENSION && colRef != null && tableMetadata != null) {
                if (excludedChecker.isExcludedCol(colRef)
                        && !colRef.getTableRef().getTableIdentity().equals(getLazyModel().getRootFactTableName())) {
                    simplifiedNamedColumn.setExcluded(true);
                }
                TableExtDesc tableExt = tableMetadata.getTableExtIfExists(colRef.getTableRef().getTableDesc());
                TableExtDesc.ColumnStats columnStats = Objects.isNull(tableExt) ? null
                        : tableExt.getColumnStatsByName(colRef.getName());
                if (columnStats != null) {
                    simplifiedNamedColumn.setCardinality(columnStats.getCardinality());
                }

            }
            selectedColumns.add(simplifiedNamedColumn);
        }

        return selectedColumns;
    }

    public void computedInfo(long inconsistentCount, ModelStatusToDisplayEnum status, boolean isScd2,
            NDataModel modelDesc, boolean onlyNormalDim) {
        if (!onlyNormalDim) {
            this.enrichDerivedDimension();
        }
        this.setForbiddenOnline(isScd2);
        this.setStatus(status);
        this.setInconsistentSegmentCount(inconsistentCount);
        computedDisplayInfo(modelDesc);
    }

    protected void computedDisplayInfo(NDataModel modelDesc) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), this.getProject());
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                this.getProject());
        this.setLastBuildTime(dfManager.getDataflowLastBuildTime(modelDesc.getUuid()));
        this.setStorage(dfManager.getDataflowStorageSize(modelDesc.getUuid()));
        this.setSource(dfManager.getDataflowSourceSize(modelDesc.getUuid()));
        this.setSegmentHoles(dfManager.calculateSegHoles(modelDesc.getUuid()));
        this.setExpansionrate(ModelUtils.computeExpansionRate(this.getStorage(), this.getSource()));
        this.setUsage(dfManager.getDataflow(modelDesc.getUuid()).getQueryHitCount());
        if (!modelDesc.isBroken()) {
            IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelDesc.getUuid());
            this.setAvailableIndexesCount(indexPlanManager.getAvailableIndexesCount(getProject(), modelDesc.getId()));
            this.setTotalIndexes(indexPlan.getAllLayoutsReadOnly().size());
            this.setEmptyIndexesCount(this.totalIndexes - this.availableIndexesCount);
            this.setHasBaseAggIndex(indexPlan.containBaseAggLayout());
            this.setHasBaseTableIndex(indexPlan.containBaseTableLayout());
        }
    }
}
