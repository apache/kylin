/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.security;

import com.kylinolap.common.persistence.RootPersistentEntity;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.job.JobInstance;
import com.kylinolap.metadata.project.ProjectInstance;

/**
 * @author xduo
 * 
 */
public class AclEntityFactory {

    public static RootPersistentEntity createAclEntity(String entityType, String uuid) {
        if ("CubeInstance".equals(entityType)) {
            CubeInstance cubeInstance = new CubeInstance();
            cubeInstance.setUuid(uuid);

            return cubeInstance;
        }

        if ("JobInstance".equals(entityType)) {
            JobInstance jobInstance = new JobInstance();
            jobInstance.setUuid(uuid);

            return jobInstance;
        }

        if ("ProjectInstance".equals(entityType)) {
            ProjectInstance projectInstance = new ProjectInstance();
            projectInstance.setUuid(uuid);

            return projectInstance;
        }

        throw new RuntimeException("Unsupported entity type!");
    }
}
