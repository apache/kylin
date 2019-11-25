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

package org.apache.kylin.engine.spark.metadata.cube;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.MetadataConstants;

import static org.apache.kylin.engine.spark.metadata.cube.model.Cube.CUBE_RESOURCE_ROOT;

public class PathManager {

    private String projectName = "default";
    private String cubeId;

    public PathManager(String projectName, String cubeId) {
        this.projectName = projectName;
        this.cubeId = cubeId;
    }

    public String getCubePath() {
        return "/" + projectName + CUBE_RESOURCE_ROOT + "/" + cubeId + MetadataConstants.FILE_SURFIX;
    }

    public String getModelPath() {
        return new StringBuilder().append("/").append(projectName).append(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT)
                .append("/").append(cubeId).append(MetadataConstants.FILE_SURFIX).toString();
    }
}
