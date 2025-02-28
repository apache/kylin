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

package org.apache.kylin.rec.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.utils.DateUtils;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;

@Getter
@Setter
@NoArgsConstructor
public class QueryRecItem implements Serializable {
    private static final int DEATH_LOOP_DETECT = 10000;
    private String queryId;
    private String sql;

    private long totalScanBytes;
    private long totalScanCount;
    private long resultRowCount;
    private long cpuTime;
    private long duration;

    private String project;
    private String modelIds;

    private String submitter;
    private String server;
    private String errorType;
    private String engineType;

    private boolean cacheHit;
    private boolean indexHit;
    private boolean isTableIndexUsed;
    private boolean isAggIndexUsed;
    private boolean isTableSnapshotUsed;

    private String queryStatus;

    private long queryTime;
    private long queryDay;
    private long queryFirstDayOfMonth;
    private long queryFirstDayOfWeek;
    private String day;

    private boolean isAccelerated = true;
    private String accFailedReason;
    private Map<String, RecDetail> recDetailMap = Maps.newHashMap();

    @SneakyThrows
    public QueryRecItem(QueryHistory queryHistory) {
        this.queryId = queryHistory.getQueryId();
        this.sql = queryHistory.getQueryHistorySql().getNormalizedSql();
        this.project = queryHistory.getProjectName();
        this.submitter = queryHistory.getQuerySubmitter();
        this.server = queryHistory.getHostName();

        this.errorType = queryHistory.getErrorType();
        this.engineType = queryHistory.getEngineType();
        this.queryStatus = queryHistory.getQueryStatus();

        this.cacheHit = queryHistory.isCacheHit();
        this.indexHit = queryHistory.isIndexHit();
        this.isTableIndexUsed = queryHistory.isIndexHit();

        this.queryTime = queryHistory.getQueryTime();
        Date date = new Date(queryTime);
        this.day = DateUtils.formatDate(date, "yyyy-MM-dd");
        this.queryDay = TimeUtil.getDayStart(queryTime);
        this.queryFirstDayOfMonth = TimeUtil.getMonthStart(queryTime);
        this.queryFirstDayOfWeek = TimeUtil.getWeekStart(queryTime);

        this.totalScanBytes = queryHistory.getTotalScanBytes();
        this.totalScanCount = queryHistory.getTotalScanCount();
        this.resultRowCount = queryHistory.getResultRowCount();
        this.cpuTime = queryHistory.getCpuTime() == null ? 0 : queryHistory.getCpuTime();
        this.duration = queryHistory.getDuration();

    }

    public QueryRecItem.RecDetail getRecDetail(NDataModel model) {
        QueryRecItem.RecDetail recDetail;
        if (getRecDetailMap().containsKey(model.getId())) {
            recDetail = getRecDetailMap().get(model.getId());
        } else {
            recDetail = new QueryRecItem.RecDetail(model);
            getRecDetailMap().put(model.getId(), recDetail);
        }
        return recDetail;
    }

    public void addLayout(LayoutEntity layout, NDataModel model) {
        getRecDetail(model).getLayouts().add(new RecInfo(layout, model, Collections.emptyMap()));
    }

    public void addLayoutRec(LayoutEntity layout, NDataModel model, Map<String, RawRecItem> nonLayoutRecItemIdMap) {
        try {
            getRecDetail(model).getLayoutRecs().add(new RecInfo(layout, model, nonLayoutRecItemIdMap));
        }catch (RuntimeException e){
            // ignore this layout rec
        }
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class RecDetail implements Serializable {
        private String modelId;
        private int semanticVersion;
        private List<RecInfo> layouts = new ArrayList<>();
        private List<RecInfo> layoutRecs = new ArrayList<>();

        public RecDetail(NDataModel dataModel) {
            this.modelId = dataModel.getUuid();
            this.semanticVersion = dataModel.getSemanticVersion();
        }

    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class RecInfo implements Serializable {
        @JsonIgnore
        private transient Map<String, RawRecItem> nonLayoutRecItemMap;
        @JsonIgnore
        private transient NDataModel dataModel;
        private String uniqueId;
        private List<String> columns = new ArrayList<>();
        private List<String> dimensions = new ArrayList<>();
        private List<NDataModel.Measure> measures = new ArrayList<>();
        private List<String> shardBy = new ArrayList<>();
        private List<String> sortBy = new ArrayList<>();
        private Map<String, String> ccExpression = new HashMap<>();
        private Map<String, String> ccType = new HashMap<>();

        public RecInfo(LayoutEntity layoutEntity, NDataModel dataModel, Map<String, RawRecItem> nonLayoutRecItemMap) {
            this.dataModel = dataModel;
            this.nonLayoutRecItemMap = nonLayoutRecItemMap;
            for (Integer colId : layoutEntity.getColOrder()) {
                acquireDependencies(colId, new AtomicInteger());
            }
            for (Integer colId : layoutEntity.getShardByColumns()) {
                this.shardBy.add(getColumnNameById(colId));
            }
            for (Integer colId : layoutEntity.getShardByColumns()) {
                this.sortBy.add(getColumnNameById(colId));
            }
            this.initUniqueId();
        }

        private void initUniqueId() {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            for (String dim : dimensions) {
                if (!first) {
                    builder.append(',');
                }
                first = false;
                builder.append(dim);
            }
            builder.append('@');
            first = true;
            for (NDataModel.Measure measure : measures) {
                if (!first) {
                    builder.append(',');
                }
                first = false;
                builder.append(measure.getFunction().getFullExpression());
            }
            builder.append('@');
            for (String shardByCol : shardBy) {
                builder.append(',').append(shardByCol);
            }
            builder.append('@');
            for (String sortByCol : sortBy) {
                builder.append(',').append(sortByCol);
            }
            this.uniqueId = RawRecUtil.computeMD5(builder.toString());
        }

        private void addColumn(TblColRef column) {
            if (!columns.contains(column.getIdentity())) {
                columns.add(column.getIdentity());
            }
        }

        private void addColumn(String column) {
            if (!columns.contains(column)) {
                columns.add(column);
            }
        }

        private void addColumn(NDataModel.NamedColumn column) {
            TblColRef col = dataModel.getColRef(column.getAliasDotColumn());
            if (col.getColumnDesc().isComputedColumn()) {
                addColumnRelatedCC(col);
            } else {
                addColumn(col);
            }
        }

        private void addMeasure(NDataModel.Measure measure) {
            measures.add(measure);
            for (ParameterDesc parameterDesc : measure.getFunction().getParameters()) {
                if (parameterDesc.isColumnType()) {
                    TblColRef colRef = parameterDesc.getColRef();
                    if (colRef != null) {
                        addColumn(colRef);
                    } else {
                        addColumn(parameterDesc.getValue());
                    }
                }
            }
        }

        private void addColumnRelatedCC(TblColRef col) {
            for (ComputedColumnDesc cc : dataModel.getComputedColumnDescs()) {
                if (cc.getColumnName().equals(col.getName())) {
                    addCC(cc);
                }
            }
        }

        private void addCC(ComputedColumnDesc cc) {
            if (ccExpression.containsKey(cc.getFullName())) {
                return;
            }
            ccExpression.put(cc.getFullName(), cc.getInnerExpression());
            ccType.put(cc.getFullName(), cc.getDatatype());
            val exprIdentifiers = ComputedColumnUtil.ExprIdentifierFinder.getExprIdentifiers(cc.getExpression());
            ImmutableBiMap<Integer, TblColRef> effectiveCols = dataModel.getEffectiveCols();
            Map<String, Integer> map = Maps.newHashMap();
            effectiveCols.forEach((k, v) -> map.put(v.getIdentity(), k));
            for (Pair<String, String> exprIdentifier : exprIdentifiers) {
                String columnName = exprIdentifier.getFirst() + "." + exprIdentifier.getSecond();
                addColumn(dataModel.getColRef(columnName));
            }
        }

        private void addDimension(NDataModel.NamedColumn column) {
            dimensions.add(column.getAliasDotColumn());
            addColumn(column);
        }

        private void acquireDependencies(Integer id, AtomicInteger countLoop) {
            if (countLoop.incrementAndGet() > DEATH_LOOP_DETECT || Integer.MAX_VALUE == id) {
                return;
            }
            if (id < 0) {
                // using recommend measures&dimensions&cc
                acquireRecItemDependencies(id, countLoop);
            } else {
                // using existing measures&dimensions
                if (id < NDataModel.MEASURE_ID_BASE) {
                    NDataModel.NamedColumn column = dataModel.getEffectiveNamedColumns().get(id);
                    if (countLoop.get() == 1) {
                        addDimension(column);
                    } else {
                        addColumn(column);
                    }
                } else {
                    NDataModel.Measure measure = dataModel.getEffectiveMeasures().get(id);
                    addMeasure(measure);
                }
            }
        }

        private String getColumnNameById(Integer id) {
            if (id < 0) {
                RawRecItem rawRecItem = nonLayoutRecItemMap.get(id + "");
                CCRecItemV2 cc = (CCRecItemV2) rawRecItem.getRecEntity();
                return cc.getCc().getFullName();
            } else {
                return dataModel.getEffectiveNamedColumns().get(id).getAliasDotColumn();
            }
        }

        private void acquireRecItemDependencies(Integer id, AtomicInteger countLoop) {
            RawRecItem rawRecItem = nonLayoutRecItemMap.get(id + "");
            if (rawRecItem.getRecEntity() instanceof DimensionRecItemV2) {
                DimensionRecItemV2 dimensionRec = (DimensionRecItemV2) rawRecItem.getRecEntity();
                addDimension(dimensionRec.getColumn());
            }
            if (rawRecItem.getRecEntity() instanceof CCRecItemV2) {
                CCRecItemV2 cc = (CCRecItemV2) rawRecItem.getRecEntity();
                addCC(cc.getCc());
            }
            if (rawRecItem.getRecEntity() instanceof MeasureRecItemV2) {
                MeasureRecItemV2 measure = (MeasureRecItemV2) rawRecItem.getRecEntity();
                checkIsValidMeasure(measure.getMeasure(), rawRecItem.getDependIDs());
                addMeasure(measure.getMeasure());
            }
            for (Integer dependId : rawRecItem.getDependIDs()) {
                acquireDependencies(dependId, countLoop);
            }
        }

        private void checkIsValidMeasure(NDataModel.Measure measure, int[] dependIds) {
            int index = 0;
            for (ParameterDesc parameterDesc : measure.getFunction().getParameters()) {
                if (dependIds[index] == Integer.MAX_VALUE && parameterDesc.isColumnType()) {
                    throw new KylinRuntimeException("bad measureRecItem, ignore this layout rec");
                }
                index++;
            }
        }

    }

}
