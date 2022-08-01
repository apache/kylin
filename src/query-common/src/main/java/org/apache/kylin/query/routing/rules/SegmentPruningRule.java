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
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.RealizationPruner;
import org.apache.kylin.query.routing.RoutingRule;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.NDataflow;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentPruningRule extends RoutingRule {
    @Override
    public void apply(List<Candidate> candidates) {
        val iterator = candidates.iterator();
        while (iterator.hasNext()) {
            val candidate = iterator.next();
            List<IRealization> realizations = candidate.getRealization().getRealizations();
            for (IRealization realization : realizations) {
                NDataflow df = (NDataflow) realization;
                val prunedSegments = RealizationPruner.pruneSegments(df, candidate.getCtx());
                candidate.setPrunedSegments(prunedSegments, df.isStreaming());
            }
            if (CollectionUtils.isEmpty(candidate.getPrunedSegments())
                    && CollectionUtils.isEmpty(candidate.getPrunedStreamingSegments())) {
                log.info("there is no segment to answer sql");
                val capability = new CapabilityResult();
                capability.capable = true;
                capability.setSelectedCandidate(NLayoutCandidate.EMPTY);
                capability.setSelectedStreamingCandidate(NLayoutCandidate.EMPTY);
                candidate.setCapability(capability);
            }
        }
    }
}
