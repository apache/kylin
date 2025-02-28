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

package org.apache.kylin.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
    private String type;
    private int storageType;
    private boolean isPartialMatchModel;
    private boolean isValid = true;
    private boolean isLayoutExist = true;
    private boolean isStreamingLayout = false;
    private List<String> lookupTables = new ArrayList<>();
    private long lastDataRefreshTime;
    private boolean isLoadingData;
    private boolean isBuildingIndex;

    public NativeQueryRealization(String modelId, String modelAlias, Long layoutId, String type,
            boolean isPartialMatchModel) {
        this(modelId, layoutId, type, isPartialMatchModel);
        this.modelAlias = modelAlias;
    }

    public NativeQueryRealization(String lookupTable, String type) {
        this("null", lookupTable, -1L, type, false);
    }

    public NativeQueryRealization(String modelId, Long layoutId, String type) {
        this(modelId, layoutId, type, false);
    }

    public NativeQueryRealization(String modelId, Long layoutId, String type, List<String> lookupTables) {
        this(modelId, layoutId, type, false);
        this.lookupTables = lookupTables;
    }

    public NativeQueryRealization(String modelId, Long layoutId, String type, boolean isPartialMatchModel) {
        this.modelId = modelId;
        this.layoutId = layoutId != null && layoutId == -1L ? null : layoutId;
        this.type = type;
        this.isPartialMatchModel = isPartialMatchModel;
    }

    public NativeQueryRealization(String modelId, String modelAlias, long layoutId, String type, boolean isPartialMatch,
            boolean isValid, boolean isLayoutExist) {
        this.modelId = modelId;
        this.modelAlias = modelAlias;
        this.layoutId = layoutId;
        this.type = type;
        this.isPartialMatchModel = isPartialMatch;
        this.isValid = isValid;
        this.isLayoutExist = isLayoutExist;
    }
}
