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

package org.apache.kylin.rest.response;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.PartitionDesc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by luwei on 17-4-19.
 */

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataModelDescResponse extends DataModelDesc {
    public void setProject(String project) {
        this.project = project;
    }

    public DataModelDescResponse() {

    }

    @JsonProperty("project")
    private String project;

    public DataModelDescResponse(DataModelDesc dataModelDesc) {
        setUuid(dataModelDesc.getUuid());
        setLastModified(dataModelDesc.getLastModified());
        setVersion(dataModelDesc.getVersion());
        setName(dataModelDesc.getName());
        setOwner(dataModelDesc.getOwner());
        setDraft(dataModelDesc.isDraft());
        setDescription(dataModelDesc.getDescription());
        setRootFactTableName(dataModelDesc.getRootFactTableName());
        setJoinTables(dataModelDesc.getJoinTables());
        setDimensions(dataModelDesc.getDimensions());
        setMetrics(dataModelDesc.getMetrics());
        setFilterCondition(dataModelDesc.getFilterCondition());
        if (dataModelDesc.getPartitionDesc() != null)
            setPartitionDesc(PartitionDesc.getCopyOf(dataModelDesc.getPartitionDesc()));
        setCapacity(dataModelDesc.getCapacity());
    }
}
