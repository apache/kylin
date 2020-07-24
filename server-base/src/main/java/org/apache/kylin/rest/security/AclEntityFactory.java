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

package org.apache.kylin.rest.security;

import java.util.UUID;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectInstance;

/**
 * @author xduo
 */
public class AclEntityFactory implements AclEntityType {

    public static RootPersistentEntity createAclEntity(String entityType, String uuid) {
        // Validate the uuid first, exception will be thrown if the uuid string is not a valid uuid
        UUID uuidObj = UUID.fromString(uuid);
        uuid = uuidObj.toString();

        if (CUBE_INSTANCE.equals(entityType)) {
            CubeInstance cubeInstance = new CubeInstance();
            cubeInstance.setUuid(uuid);

            return cubeInstance;
        }

        if (DATA_MODEL_DESC.equals(entityType)) {
            DataModelDesc modelInstance = new DataModelDesc();
            modelInstance.setUuid(uuid);

            return modelInstance;
        }

        if (JOB_INSTANCE.equals(entityType)) {
            JobInstance jobInstance = new JobInstance();
            jobInstance.setUuid(uuid);

            return jobInstance;
        }

        if (PROJECT_INSTANCE.equals(entityType)) {
            ProjectInstance projectInstance = new ProjectInstance();
            projectInstance.setUuid(uuid);

            return projectInstance;
        }

        throw new RuntimeException("Unsupported entity type!");
    }
}
