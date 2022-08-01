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
import java.util.stream.Collectors;

import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.streaming.constants.StreamingConstants;

public class CatchupMergePolicy extends MergePolicy {

    private boolean segSizeMatched = false;

    public List<NDataSegment> selectMatchedSegList(List<NDataSegment> segList, int layer, long thresholdOfSegSize,
            int numberOfSeg) {
        matchSegList.clear();
        segSizeMatched = false;
        int maxNumOfSeg = numberOfSeg * 5;
        int idx = findStartIndex(segList, thresholdOfSegSize);
        if (idx != -1) {
            List<NDataSegment> layerSegList = segList
                    .subList(idx, segList.size()).stream().filter(item -> item.getAdditionalInfo()
                            .getOrDefault(StreamingConstants.FILE_LAYER, "0").equals(String.valueOf(layer)))
                    .collect(Collectors.toList());
            if (layerSegList.size() >= maxNumOfSeg) {
                segSizeMatched = true;
                long totalThresholdOfSegSize = 0;
                for (int i = idx; i < segList.size(); i++) {
                    matchSegList.add(segList.get(i));
                    totalThresholdOfSegSize += segList.get(i).getStorageBytesSize();
                    if (isThresholdOfSegSizeOver(totalThresholdOfSegSize, thresholdOfSegSize)) {
                        break;
                    }
                }
                return matchSegList;
            } else {
                return Collections.emptyList();
            }
        } else {
            return Collections.emptyList();
        }
    }

    public boolean matchMergeCondition(long thresholdOfSegSize) {
        return segSizeMatched || (matchSegList.size() > 1 && isThresholdOfSegSizeOver(
                matchSegList.stream().mapToLong(NDataSegment::getStorageBytesSize).sum(), thresholdOfSegSize));
    }
}
