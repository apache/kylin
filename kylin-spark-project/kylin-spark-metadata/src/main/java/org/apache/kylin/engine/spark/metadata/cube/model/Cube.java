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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.metadata.model.TableRef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.kylin.common.persistence.ResourceStore.CUBE_RESOURCE_ROOT;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Cube extends RootPersistentEntity {

    @JsonIgnore
    private KylinConfig config;

    @JsonProperty("projefct")
    private String project;

    private DataModel dataModel;

    private List<IndexEntity> indexEntities = new ArrayList<>();

    private List<DimensionDesc> dimensions = new ArrayList<>();

    private List<MeasureDesc> measures = new ArrayList<>();

    private transient SpanningTree spanningTree = null; // transient, because can self recreate

    private Cube(KylinConfig config) {
        String resourceRootPath = "/" + project + CUBE_RESOURCE_ROOT;
        Preconditions.checkArgument(resourceRootPath.equals("") || resourceRootPath.startsWith("/"));
        Preconditions.checkArgument(!resourceRootPath.endsWith("/"));
    }

    public static Cube getInstance(KylinConfig config) {
        return new Cube(config);
    }

    //add layout when build cube
    public Cube updateCube(IndexEntity[] indexes) {
        List<IndexEntity> indexEntities = Arrays.asList(indexes);
        this.addIndexEntities(indexEntities);
        return this;
    }
  
    public DataModel getDataModel() {
        return dataModel;
    }

    public void setDataModel(DataModel dataModel) {
        this.dataModel = dataModel;
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

    public void setSpanningTree(SpanningTree spanningTree) {
        this.spanningTree = spanningTree;
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

    //used to dump resource before spark job submit
    public Set<String> collectPrecalculationResource() {
        Set<String> r = new LinkedHashSet<>();

        // cube & segments
        r.add(PathManager.getCubePath(project, getId()));

        // project & model & tables
        r.add(PathManager.getProjectPath(project));
        r.add(PathManager.getModelPath(project, getId()));
        for (TableRef t : getDataModel().getAllTableRefs()) {
            r.add(t.getTableDesc().getResourcePath());
        }

        return r;
    }

    public KylinConfigExt getConfig() {
        return (KylinConfigExt) config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

    public List<IndexEntity> getIndexEntities() {
        return indexEntities;
    }

    public void setIndexEntities(List<IndexEntity> indexEntities) {
        this.indexEntities = indexEntities;
    }

    public void addIndexEntities(List<IndexEntity> indexEntities) {
        this.getIndexEntities().addAll(indexEntities);
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

    public LayoutEntity getCuboidLayout(Long cuboidLayoutId) {
        return getSpanningTree().getCuboidLayout(cuboidLayoutId);
    }

}
