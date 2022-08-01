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

package org.apache.kylin.query.routing.rules;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.RealizationPruner;
import org.apache.kylin.query.routing.RoutingRule;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.NDataflowManager;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PartitionPruningRule extends RoutingRule {
    @Override
    public void apply(List<Candidate> candidates) {
        val iterator = candidates.iterator();
        while (iterator.hasNext()) {
            val candidate = iterator.next();
            val model = candidate.getRealization().getModel();

            // no segment can answer
            val prunedSegments = candidate.getPrunedSegments();
            if (CollectionUtils.isEmpty(prunedSegments)) {
                continue;
            }

            // does not define any second-level partition
            val multiPartitionDesc = model.getMultiPartitionDesc();
            if (multiPartitionDesc == null || CollectionUtils.isEmpty(multiPartitionDesc.getColumns())) {
                continue;
            }

            val matchedPartitions = RealizationPruner.matchPartitions(prunedSegments, model, candidate.getCtx());

            // push down
            if (matchedPartitions == null) {
                val capability = new CapabilityResult();
                capability.capable = false;
                candidate.setCapability(capability);
                iterator.remove();
                continue;
            }

            val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                    .getDataflow(model.getId());
            val allPartitionsEmpty = matchedPartitions.entrySet().stream().allMatch(entry -> {
                val segment = dataflow.getSegment(entry.getKey());
                val partitionIds = entry.getValue();
                return CollectionUtils.isNotEmpty(segment.getMultiPartitionIds())
                        && CollectionUtils.isEmpty(partitionIds);
            });

            // return empty result
            if (allPartitionsEmpty) {
                log.info("there is no partition to answer sql");
                val capability = new CapabilityResult();
                capability.capable = true;
                capability.setSelectedCandidate(NLayoutCandidate.EMPTY);
                candidate.setCapability(capability);
                continue;
            }

            candidate.setPrunedPartitions(matchedPartitions);
        }
    }
}
