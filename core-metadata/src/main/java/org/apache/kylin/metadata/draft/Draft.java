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

package org.apache.kylin.metadata.draft;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class Draft extends RootPersistentEntity {

    @JsonProperty("project")
    private String project;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    @JsonProperty("draft_entities")
    private RootPersistentEntity[] draftEntities;

    // ============================================================================

    public Draft() {
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public RootPersistentEntity[] getEntities() {
        return draftEntities;
    }

    public void setEntities(RootPersistentEntity[] draftEntities) {
        this.draftEntities = draftEntities;
    }

    public RootPersistentEntity getEntity() {
        return getEntities()[0];
    }

    public void setEntity(RootPersistentEntity entity) {
        setEntities(new RootPersistentEntity[] { entity });
    }

    public String getResourcePath() {
        return concatResourcePath(uuid);
    }

    public static String concatResourcePath(String uuid) {
        return ResourceStore.DRAFT_RESOURCE_ROOT + "/" + uuid + ".json";
    }

    @Override
    public String toString() {
        return "Draft [project=" + project + ", uuid=" + uuid + "]";
    }

}
