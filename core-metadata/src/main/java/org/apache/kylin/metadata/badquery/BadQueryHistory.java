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

package org.apache.kylin.metadata.badquery;

import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class BadQueryHistory extends RootPersistentEntity {

    @JsonProperty("project")
    private String project;
    @JsonProperty("entries")
    private NavigableSet<BadQueryEntry> entries = new TreeSet<>();

    public BadQueryHistory() {
    }

    public BadQueryHistory(String project) {
        this.updateRandomUuid();
        this.project = project;
    }

    public NavigableSet<BadQueryEntry> getEntries() {
        return entries;
    }

    public void setEntries(NavigableSet<BadQueryEntry> entries) {
        this.entries = entries;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getResourcePath() {
        return ResourceStore.BAD_QUERY_RESOURCE_ROOT + "/" + project + MetadataConstants.FILE_SURFIX;
    }

    @Override
    public String toString() {
        return "BadQueryHistory [ project=" + project + "]";
    }
}
