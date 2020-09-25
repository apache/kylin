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

package org.apache.kylin.stream.core.consumer;

import java.util.List;
import java.util.Set;

import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.source.ISourcePosition;
import org.apache.kylin.stream.core.source.ISourcePosition.IPartitionPosition;
import org.apache.kylin.stream.core.source.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class EndPositionStopCondition implements IStopConsumptionCondition {
    private static final Logger logger = LoggerFactory.getLogger(EndPositionStopCondition.class);
    private ISourcePosition endPosition;
    private Set<Integer> finishPartitions;
    private int expectedEndPartitionNum;

    public EndPositionStopCondition(ISourcePosition endPosition) {
        this.finishPartitions = Sets.newHashSet();
        this.endPosition = endPosition;
    }

    @Override
    public void init(List<Partition> consumingPartitions) {
        // remove the partitions that is not in consumption
        Set<Integer> partitionIDSets = Sets.newHashSet();
        for (Partition partition : consumingPartitions) {
            partitionIDSets.add(partition.getPartitionId());
        }
        List<Integer> endPartitionIdList = Lists.newArrayList(endPosition.getPartitionPositions().keySet());
        for (Integer endPartitionId : endPartitionIdList) {
            if (partitionIDSets.contains(endPartitionId)) {
                expectedEndPartitionNum++;
            }
        }
    }

    @Override
    public boolean isSatisfied(StreamingMessage event) {
        if (endPosition == null) {
            return false;
        }
        IPartitionPosition partitionPosition = event.getSourcePosition();
        int partition = partitionPosition.getPartition();
        IPartitionPosition endPartPos = endPosition.getPartitionPositions().get(partition);
        if (endPartPos == null) {
            return false;
        }
        boolean exceed = (partitionPosition.compareTo(endPartPos) > 0);
        if (exceed) {
            event.setFiltered(true);
            if (!finishPartitions.contains(partition)) {
                finishPartitions.add(partition);
                logger.info("finished partitions: " + finishPartitions);
            }
        }
        return finishPartitions.size() == expectedEndPartitionNum;
    }

    @Override
    public String toString() {
        return "EndPositionStopCondition{" + "partitionEndOffsets=" + endPosition + '}';
    }
}
