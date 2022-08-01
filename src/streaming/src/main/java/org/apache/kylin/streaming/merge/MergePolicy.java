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

package org.apache.kylin.streaming.merge;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.NDataSegment;

public abstract class MergePolicy {
    protected List<NDataSegment> matchSegList = new ArrayList<>();
    private double mergeRatio = KylinConfig.getInstanceFromEnv().getStreamingSegmentMergeRatio();

    protected int findStartIndex(List<NDataSegment> segList, Long thresholdOfSegSize) {
        for (int i = 0; i < segList.size(); i++) {
            if (segList.get(i).getStorageBytesSize() > thresholdOfSegSize) {
                continue;
            } else {
                return i;
            }
        }
        return -1;
    }

    public void next(AtomicInteger currLayer) {

    }

    public abstract List<NDataSegment> selectMatchedSegList(List<NDataSegment> segList, int layer,
            long thresholdOfSegSize, int numOfSeg);

    public abstract boolean matchMergeCondition(long thresholdOfSegSize);

    public boolean isThresholdOfSegSizeOver(long totalSegSize, long thresholdOfSegSize) {
        return totalSegSize >= thresholdOfSegSize * mergeRatio;
    }

    public List<NDataSegment> getMatchSegList() {
        return matchSegList;
    }
}
