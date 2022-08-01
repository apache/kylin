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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.cube.cuboid.CuboidStatus;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Getter
@Setter
public class IndicesResponse {

    public static final String STORAGE_SIZE = "storage_size";
    public static final String QUERY_HIT_COUNT = "query_hit_count";
    public static final String LAST_MODIFY_TIME = "last_modify_time";

    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("end_time")
    private long endTime;
    @JsonProperty("indices")
    private List<Index> indices = Lists.newArrayList();
    @JsonProperty("size")
    private int size;

    @JsonIgnore
    private IndexPlan indexPlan;

    @JsonIgnore
    private NDataflow dataFlow;

    @JsonIgnore
    private Segments<NDataSegment> nonNewSegments;

    @JsonIgnore
    private boolean isAnySegReady;

    @JsonIgnore // work around with lombok
    public boolean isAnySegReady() {
        return isAnySegReady;
    }

    @JsonIgnore // work around with lombok
    public void setAnySegReady(boolean anySegReady) {
        isAnySegReady = anySegReady;
    }

    public IndicesResponse(IndexPlan indexPlan) {
        if (Objects.isNull(indexPlan)) {
            return;
        }
        this.indexPlan = indexPlan;
        this.dataFlow = NDataflowManager.getInstance(indexPlan.getConfig(), indexPlan.getProject())
                .getDataflow(indexPlan.getId());
        this.nonNewSegments = SegmentUtil.getSegmentsExcludeRefreshingAndMerging(this.dataFlow.getSegments()).stream()
                .filter(seg -> SegmentStatusEnum.NEW != seg.getStatus())
                .collect(Collectors.toCollection(Segments::new));

        this.isAnySegReady = nonNewSegments.stream().map(NDataSegment::getStatus)
                .anyMatch(status -> SegmentStatusEnum.READY == status);
        if (!isAnySegReady) {
            return;
        }
        startTime = Long.MAX_VALUE;
        for (NDataSegment seg : nonNewSegments) {
            long start = Long.parseLong(seg.getSegRange().getStart().toString());
            long end = Long.parseLong(seg.getSegRange().getEnd().toString());
            startTime = startTime < start ? startTime : start;
            endTime = endTime > end ? endTime : end;
        }
    }

    public boolean addIndexEntity(IndexEntity indexEntity) {
        if (Objects.isNull(indexPlan) || Objects.isNull(indexEntity)) {
            return false;
        }
        return indices.add(new Index(this, indexEntity));
    }

    @Getter
    @Setter
    public static class Index {
        @JsonProperty("id")
        private long id;
        @JsonProperty(STORAGE_SIZE)
        private long storageSize;
        @JsonProperty("index_type")
        private String indexType;
        @JsonProperty(QUERY_HIT_COUNT)
        private long queryHitCount;

        @JsonProperty("dimensions")
        private List<String> dimensions = Lists.newArrayList();

        @JsonProperty("measures")
        private List<String> measures = Lists.newArrayList();

        @JsonProperty("status")
        private CuboidStatus status = CuboidStatus.AVAILABLE;
        @JsonProperty(LAST_MODIFY_TIME)
        private long lastModifiedTime;

        @JsonManagedReference
        @JsonProperty("layouts")
        private List<LayoutEntity> layouts = Lists.newArrayList();

        private static final String INDEX_TYPE_AUTO = "AUTO";
        private static final String INDEX_TYPE_MANUAL = "MANUAL";

        public Index(IndicesResponse indicesResponse, IndexEntity indexEntity) {
            this.setId(indexEntity.getId());
            this.setLayouts(indexEntity.getLayouts());

            setDimensionsAndMeasures(indexEntity);
            if (!indicesResponse.isAnySegReady()) {
                status = CuboidStatus.EMPTY;
                return;
            }
            for (NDataSegment segment : indicesResponse.getNonNewSegments()) {
                for (LayoutEntity layout : layouts) {
                    NDataLayout dataLayout = segment.getLayout(layout.getId());
                    if (Objects.isNull(dataLayout)) {
                        status = CuboidStatus.EMPTY;
                        return;
                    }
                    if (Objects.isNull(indexType) && layout.isAuto()) {
                        indexType = INDEX_TYPE_AUTO;
                    }
                    this.storageSize += dataLayout.getByteSize();
                }
            }

            indexType = Objects.isNull(indexType) ? INDEX_TYPE_MANUAL : indexType;

            this.lastModifiedTime = layouts.stream().map(LayoutEntity::getUpdateTime).max(Comparator.naturalOrder())
                    .orElse(0L);
            val layoutSet = layouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
            this.queryHitCount = indicesResponse.getDataFlow().getLayoutHitCount().entrySet().stream()
                    .filter(entry -> layoutSet.contains(entry.getKey())).map(Map.Entry::getValue)
                    .mapToInt(hit -> hit.getFrequency(indexEntity.getIndexPlan().getProject())).sum();

        }

        private void setDimensionsAndMeasures(IndexEntity indexEntity) {
            ImmutableSet<TblColRef> dimensionSet = indexEntity.getDimensionSet();
            if (!CollectionUtils.isEmpty(dimensionSet)) {
                for (TblColRef dimension : dimensionSet) {
                    this.dimensions.add(dimension.getName());
                }
            }

            ImmutableSet<NDataModel.Measure> measureSet = indexEntity.getMeasureSet();
            if (!CollectionUtils.isEmpty(measureSet)) {
                for (NDataModel.Measure measure : measureSet) {
                    this.measures.add(measure.getName());
                }
            }
        }
    }

}
