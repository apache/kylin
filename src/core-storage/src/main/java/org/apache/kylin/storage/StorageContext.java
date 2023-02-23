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

package org.apache.kylin.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.NDataSegment;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xjiang
 */
@Slf4j
public class StorageContext {
    @Getter
    private int ctxId;

    @Getter
    @Setter
    private Long layoutId = -1L;

    @Getter
    @Setter
    private Long streamingLayoutId = -1L;

    @Setter
    @Getter
    private boolean partialMatchModel = false;

    @Setter
    private NLayoutCandidate candidate;

    @Getter
    @Setter
    private boolean isFilterCondAlwaysFalse;

    public NLayoutCandidate getCandidate() {
        if (isBatchCandidateEmpty() && !isStreamCandidateEmpty()) {
            return streamingCandidate;
        }
        return candidate == null ? NLayoutCandidate.EMPTY : candidate;
    }

    @Setter
    private NLayoutCandidate streamingCandidate;

    public NLayoutCandidate getStreamingCandidate() {
        return streamingCandidate == null ? NLayoutCandidate.EMPTY : streamingCandidate;
    }

    public boolean isBatchCandidateEmpty() {
        return candidate == null || candidate == NLayoutCandidate.EMPTY;
    }

    public boolean isStreamCandidateEmpty() {
        return streamingCandidate == null || streamingCandidate == NLayoutCandidate.EMPTY;
    }

    @Getter
    @Setter
    private Set<TblColRef> dimensions;

    @Getter
    @Setter
    private Set<FunctionDesc> metrics;

    @Getter
    @Setter
    private boolean useSnapshot = false;

    @Getter
    @Setter
    private List<NDataSegment> prunedSegments;

    @Getter
    @Setter
    private List<NDataSegment> prunedStreamingSegments;

    @Getter
    @Setter
    private Map<String, List<Long>> prunedPartitions;

    @Getter
    @Setter
    private boolean isEmptyLayout;

    public StorageContext(int ctxId) {
        this.ctxId = ctxId;
    }

}
