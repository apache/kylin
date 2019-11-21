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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.metadata.ModelDesc;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.kylin.metadata.MetadataConstants.FILE_SURFIX;

public class Cube {
    public static final String CUBE_RESOURCE_ROOT = "/cube";

    @JsonProperty("uuid")
    protected String uuid = UUID.randomUUID().toString();
    private KylinConfig config;
    private String project;
    private ModelDesc modelDesc;
    private List<IndexEntity> IndexEntities = new ArrayList<>();
    private List<DimensionDesc> dimensions = new ArrayList<>();
    private List<MeasureDesc> measures = new ArrayList<>();

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
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }
    public static String concatResourcePath(String name, String project) {
        return "/" + project + CUBE_RESOURCE_ROOT + "/" + name + FILE_SURFIX;
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

    public KylinConfig getConfig() {
        return config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

    public ModelDesc getModelDesc() {
        return modelDesc;
    }

    public void setModelDesc(ModelDesc modelDesc) {
        this.modelDesc = modelDesc;
    }

    public List<IndexEntity> getIndexEntities() {
        return IndexEntities;
    }

    public void setIndexEntities(List<IndexEntity> indexEntities) {
        IndexEntities = indexEntities;
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
