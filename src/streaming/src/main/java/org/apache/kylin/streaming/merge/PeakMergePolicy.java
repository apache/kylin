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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.streaming.constants.StreamingConstants;

import lombok.val;
import lombok.var;

public class PeakMergePolicy extends MergePolicy {

    public List<NDataSegment> selectMatchedSegList(List<NDataSegment> segList, int layer, long thresholdOfSegSize,
            int numberOfSeg) {
        matchSegList.clear();
        int idx = findStartIndex(segList, thresholdOfSegSize);
        if (idx != -1) {
            var matched = false;
            for (int i = idx; i < segList.size(); i++) {
                matchSegList.add(segList.get(i));
                if (segList.get(i).getAdditionalInfo().getOrDefault(StreamingConstants.FILE_LAYER, "0")
                        .equals(String.valueOf(layer))) {
                    val currSegStorageSize = segList.get(i).getStorageBytesSize();
                    if (matchSegList.size() <= numberOfSeg
                            && isThresholdOfSegSizeOver(currSegStorageSize, thresholdOfSegSize)) {
                        matched = true;
                        break;
                    }
                } else if (matchSegList.size() > numberOfSeg) {
                    break;
                }
            }
            if (!matched) {
                matchSegList.clear();
            }
            return matchSegList;
        } else {
            return Collections.emptyList();
        }
    }

    public boolean matchMergeCondition(long thresholdOfSegSize) {
        return matchSegList.size() > 1 && isThresholdOfSegSizeOver(
                matchSegList.stream().mapToLong(NDataSegment::getStorageBytesSize).sum(), thresholdOfSegSize);
    }

    public void next(AtomicInteger currLayer) {
        currLayer.incrementAndGet();
    }

    public boolean isThresholdOfSegSizeOver(long totalSegSize, long thresholdOfSegSize) {
        return totalSegSize >= thresholdOfSegSize;
    }
}
