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
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.engine.spark.metadata.cube.ManagerHub;

import java.util.ArrayList;
import java.util.List;

/**
 * Hold pre-calculated information of cuboids in CubeSegments
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataSegDetails extends RootPersistentEntity {
    public static DataSegDetails newSegDetails(Cube cube, String segId) {
        DataSegDetails dataSegDetails = new DataSegDetails();
        dataSegDetails.setConfig(cube.getConfig());
        dataSegDetails.setUuid(segId);
        dataSegDetails.setCubeId(cube.getUuid());
        dataSegDetails.setProject(cube.getProject());

        List<DataLayout> cuboids = new ArrayList<>();
        dataSegDetails.setLayouts(cuboids);
        return dataSegDetails;
    }

    @JsonProperty("cube")
    private String cubeId;

    @JsonManagedReference
    @JsonProperty("layout_instances")
    private List<DataLayout> layouts = Lists.newArrayList();

    @JsonIgnore
    private KylinConfigExt config;

    private String project;

    public Cube getCube() {
        return ManagerHub.getCube(config, cubeId);
    }

    public KylinConfigExt getConfig() {
        return config;
    }

    void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public String getCubeId() {
        return cubeId;
    }

    public void setCubeId(String cubeId) {
        this.cubeId = cubeId;
    }

    public List<DataLayout> getLayouts() {
        return layouts;
    }

    public void setLayouts(List<DataLayout> layouts) {
        this.layouts = layouts;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public DataSegment getDataSegment() {
        return getCube().getSegment(uuid);
    }

    public DataLayout getLayoutById(long layoutId) {
        for (DataLayout cuboid : getLayouts()) {
            if (cuboid.getLayoutId() == layoutId)
                return cuboid;
        }
        return null;
    }
}
