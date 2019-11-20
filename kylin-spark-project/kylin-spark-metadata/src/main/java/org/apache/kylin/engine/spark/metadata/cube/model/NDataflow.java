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

package org.apache.kylin.engine.spark.metadata.cube.model;

import org.apache.kylin.engine.spark.metadata.LayoutEntity;
import org.apache.kylin.metadata.model.TableRef;
import org.apche.kylin.engine.spark.common.persistence.RootPersistentEntity;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class NDataflow extends RootPersistentEntity {

    public static final String DATAFLOW_RESOURCE_ROOT = "/dataflow";
    public static final String FILE_SURFIX = ".json";

    private String project;

    private List<LayoutEntity> layoutEntities;

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public List<LayoutEntity> getLayoutEntities() {
        return layoutEntities;
    }

    public void setLayoutEntities(List<LayoutEntity> layoutEntities) {
        this.layoutEntities = layoutEntities;
    }

    //used to dump resource before spark job submit
    public String getResourcePath() {
        return concatResourcePath(getId(), project);
    }
    public static String concatResourcePath(String name, String project) {
        return "/" + project + DATAFLOW_RESOURCE_ROOT + "/" + name + FILE_SURFIX;
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
}
