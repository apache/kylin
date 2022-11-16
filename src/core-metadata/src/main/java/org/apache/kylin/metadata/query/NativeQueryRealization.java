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

package org.apache.kylin.metadata.query;

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.metadata.acl.NDataModelAclParams;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class NativeQueryRealization implements Serializable {
    private String modelId;
    private String modelAlias;
    private Long layoutId;
    private String indexType;
    private boolean isPartialMatchModel;
    private boolean isValid = true;
    private boolean isLayoutExist = true;
    private boolean isSecondStorage = false;
    private boolean recommendSecondStorage = false;
    private boolean isStreamingLayout = false;
    private List<String> snapshots;

    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelAclParams aclParams;

    public NativeQueryRealization(String modelId, String modelAlias, Long layoutId, String indexType,
            boolean isPartialMatchModel, List<String> snapshots) {
        this.modelId = modelId;
        this.modelAlias = modelAlias;
        this.layoutId = layoutId;
        this.indexType = indexType;
        this.isPartialMatchModel = isPartialMatchModel;
        this.snapshots = snapshots;
    }

    public NativeQueryRealization(String modelId, Long layoutId, String indexType) {
        this.modelId = modelId;
        this.layoutId = layoutId;
        this.indexType = indexType;
        this.isPartialMatchModel = false;
    }

    public NativeQueryRealization(String modelId, Long layoutId, String indexType, List<String> snapshots) {
        this.modelId = modelId;
        this.layoutId = layoutId;
        this.indexType = indexType;
        this.snapshots = snapshots;
        this.isPartialMatchModel = false;
    }

    public NativeQueryRealization(String modelId, String modelAlias, Long layoutId, String indexType) {
        this.modelId = modelId;
        this.layoutId = layoutId;
        this.modelAlias = modelAlias;
        this.indexType = indexType;
        this.isPartialMatchModel = false;
    }

    public NativeQueryRealization(String modelId, Long layoutId, String indexType, boolean isPartialMatchModel) {
        this.modelId = modelId;
        this.layoutId = layoutId;
        this.indexType = indexType;
        this.isPartialMatchModel = isPartialMatchModel;
    }
}
