/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.Segments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kylin.metadata.MetadataConstants.FILE_SURFIX;

public class Cube extends RootPersistentEntity {
    public static final String CUBE_RESOURCE_ROOT = "/cube";

    @JsonIgnore
    private KylinConfig config;

    @JsonProperty("projefct")
    private String project;

    @JsonProperty("")
    private DataModel dataModel;

    @JsonManagedReference
    @JsonProperty("segments")
    private List<DataSegment> segments = new ArrayList<>();

    private Cube(KylinConfig config) {}

    private transient SpanningTree spanningTree = null; // transient, because can self recreate

    @JsonProperty("indexes")
    private List<IndexEntity> IndexEntities = new ArrayList<>();

    private List<DimensionDesc> dimensions = new ArrayList<>();

    private List<MeasureDesc> measures = new ArrayList<>();

    public static Cube getInstance(KylinConfig config) {
        return new Cube(config);
    }

    //add layout when build cube
    public Cube updateCube(IndexEntity[] indexes) {
        List<IndexEntity> indexEntities = Arrays.asList(indexes);
        this.addIndexEntities(indexEntities);
        return this;
    }

    public DataSegment getSegment(String segId) {
        for (DataSegment seg : segments) {
            if (seg.getId().equals(segId))
                return seg;
        }
        return null;
    }
  
    public DataModel getDataModel() {
        return dataModel;
    }

    public void setDataModel(DataModel dataModel) {
        this.dataModel = dataModel;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getId() {
        return uuid;
    }

    public SpanningTree getSpanningTree() {
        if (spanningTree != null)
            return spanningTree;

        synchronized (this) {
            if (spanningTree == null) {
                spanningTree = SpanningTreeFactory.fromCube(this);
            }
            return spanningTree;
        }
    }

    //used to dump resource before spark job submit
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return "/" + project + CUBE_RESOURCE_ROOT + "/" + name + FILE_SURFIX;
    }

    public List<IndexEntity> getAllIndexes() {
        Map<Long, Integer> retSubscriptMap = Maps.newHashMap();
        List<IndexEntity> mergedIndexes = Lists.newArrayList();
        int retSubscript = 0;
        for (IndexEntity indexEntity : IndexEntities) {
            IndexEntity copy = JsonUtil.deepCopyQuietly(indexEntity, IndexEntity.class);
            retSubscriptMap.put(indexEntity.getId(), retSubscript);
            mergedIndexes.add(copy);
            retSubscript++;
        }

        //TODO: comment out for now
//        for (LayoutEntity ruleBasedLayout : ruleBasedLayouts) {
//            val ruleRelatedIndex = ruleBasedLayout.getIndex();
//            if (!retSubscriptMap.containsKey(ruleRelatedIndex.getId())) {
//                val copy = JsonUtil.deepCopyQuietly(ruleRelatedIndex, IndexEntity.class);
//                retSubscriptMap.put(ruleRelatedIndex.getId(), retSubscript);
//                mergedIndexes.add(copy);
//                retSubscript++;
//            }
//            val subscript = retSubscriptMap.get(ruleRelatedIndex.getId());
//            val targetIndex = mergedIndexes.get(subscript);
//            val originLayouts = targetIndex.getLayouts();
//            boolean isMatch = originLayouts.stream().filter(originLayout -> originLayout.equals(ruleBasedLayout))
//                    .peek(originLayout -> originLayout.setManual(true)).count() > 0;
//            LayoutEntity copyRuleBasedLayout;
//            copyRuleBasedLayout = JsonUtil.deepCopyQuietly(ruleBasedLayout, LayoutEntity.class);
//            if (!isMatch) {
//                originLayouts.add(copyRuleBasedLayout);
//            }
//            targetIndex.setLayouts(originLayouts);
//            targetIndex.setNextLayoutOffset(Math.max(targetIndex.getNextLayoutOffset(),
//                    originLayouts.stream().mapToLong(LayoutEntity::getId).max().orElse(0) % INDEX_ID_STEP + 1));
//            copyRuleBasedLayout.setIndex(targetIndex);
//        }

        mergedIndexes.forEach(value -> value.setCube(this));
        return mergedIndexes;

    }

    public Set<String> collectPrecalculationResource() {
        Set<String> r = new LinkedHashSet<>();

        // dataflow & segments
        r.add(this.getResourcePath());
        /*for (NDataSegment seg : segments) {
            r.add(seg.getSegDetails().getResourcePath());
        }*/

        // cubing plan
        //r.add(getIndexPlan().getResourcePath());

        // project & model & tables
        /*r.add(getModel().getProjectInstance().getResourcePath());
        r.add(getModel().getResourcePath());
        for (TableRef t : getModel().getAllTables()) {
            r.add(t.getTableDesc().getResourcePath());
        }*/

        return r;
    }

    public LayoutEntity getCuboidLayout(Long cuboidLayoutId) {
        return getSpanningTree().getCuboidLayout(cuboidLayoutId);
    }

    public KylinConfigExt getConfig() {
        return (KylinConfigExt) config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

    public List<IndexEntity> getIndexEntities() {
        return IndexEntities;
    }

    public void setIndexEntities(List<IndexEntity> indexEntities) {
        this.IndexEntities = indexEntities;
    }

    public void addIndexEntities(List<IndexEntity> indexEntities) {
        this.IndexEntities.addAll(indexEntities);
    }

    public List<DimensionDesc> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<DimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    public List<MeasureDesc> getMeasures() {
        return measures;
    }

    public void setMeasures(List<MeasureDesc> measures) {
        this.measures = measures;
    }
    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }
}
