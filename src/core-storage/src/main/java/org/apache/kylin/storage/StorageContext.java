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

import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Setter
@Getter
@Slf4j
public class StorageContext {

    private final int ctxId;

    // dimensions & metrics
    private Set<TblColRef> dimensions;
    private Set<FunctionDesc> metrics;

    // storage related info
    private NLayoutCandidate batchCandidate = NLayoutCandidate.ofEmptyCandidate();
    private NLayoutCandidate streamCandidate = NLayoutCandidate.ofEmptyCandidate();
    private NLookupCandidate lookupCandidate;
    private boolean isDataSkipped;

    // other info
    private Map<String, List<Long>> prunedPartitions;
    private boolean isPartialMatch = false;
    private boolean isFilterCondAlwaysFalse;

    public StorageContext(int ctxId) {
        this.ctxId = ctxId;
    }

    public NLookupCandidate getLookupCandidate() {
        if (streamCandidate.isEmpty() && batchCandidate.isEmpty()) {
            return lookupCandidate;
        } else {
            return null;
        }
    }

    public NLayoutCandidate getCandidate() {
        if (isBatchCandidateEmpty() && !isStreamCandidateEmpty()) {
            return streamCandidate;
        }
        return batchCandidate;
    }

    public boolean isBatchCandidateEmpty() {
        return batchCandidate.getLayoutId() == -1L;
    }

    public boolean isStreamCandidateEmpty() {
        return streamCandidate.getLayoutId() == -1L;
    }
}
