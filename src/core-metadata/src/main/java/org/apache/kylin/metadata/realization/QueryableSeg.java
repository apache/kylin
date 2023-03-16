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

package org.apache.kylin.metadata.realization;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.compress.utils.Lists;
import org.apache.kylin.metadata.cube.model.NDataSegment;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class QueryableSeg {

    private List<NDataSegment> batchSegments;

    private List<NDataSegment> streamingSegments;

    private Map<String, Set<Long>> chSegToLayoutsMap;

    public List<String> getPrunedSegmentIds(boolean isBatch) {
        List<NDataSegment> segments = isBatch ? batchSegments : streamingSegments;
        if (segments == null) {
            segments = Lists.newArrayList();
        }
        return segments.stream().map(NDataSegment::getId).collect(Collectors.toList());
    }
}
