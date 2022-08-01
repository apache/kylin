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

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.acl.NDataModelAclParams;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class RelatedModelResponse extends NDataModel {

    @JsonProperty("status")
    private RealizationStatusEnum status;
    @JsonProperty("segment_ranges")
    private Map<SegmentRange, SegmentStatusEnum> segmentRanges = new HashMap<>();
    @JsonProperty("has_error_jobs")
    private boolean hasErrorJobs;

    public RelatedModelResponse() {
        super();
    }

    public RelatedModelResponse(NDataModel dataModel) {
        super(dataModel);
        this.setMvcc(dataModel.getMvcc());
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelOldParams oldParams;

    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelAclParams aclParams;

}
