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

package org.apache.kylin.rest.reponse;

import org.apache.kylin.rest.service.OpsService;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetadataBackupResponse {
    @JsonProperty("path")
    private String path;
    @JsonProperty("status")
    private OpsService.MetadataBackupStatus status;
    @JsonProperty("size")
    private String size;
    @JsonProperty("owner")
    private String owner;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public OpsService.MetadataBackupStatus getStatus() {
        return status;
    }

    public void setStatus(OpsService.MetadataBackupStatus status) {
        this.status = status;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

}
