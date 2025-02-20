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

package org.apache.kylin.query.routing;

import java.util.List;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationCandidate;
import org.apache.kylin.metadata.realization.SQLDigest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataflowCapabilityChecker {

    private DataflowCapabilityChecker() {
    }

    public static CapabilityResult check(NDataflow dataflow, Candidate candidate, SQLDigest digest) {
        log.info("Matching Layout in dataflow {}, SQL digest {}", dataflow, digest);
        CapabilityResult result = new CapabilityResult();
        if (digest.isLimitPrecedesAggr()) {
            log.info("Exclude NDataflow {} because there's limit preceding aggregation", dataflow);
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.LIMIT_PRECEDE_AGGR);
            return result;
        }

        IRealizationCandidate chosenCandidate = null;

        // for query-on-fact-table
        log.trace("Normal dataflow matching");
        List<NDataSegment> prunedSegments = candidate.getPrunedSegments(dataflow);
        NLayoutCandidate candidateAndInfluence = QueryLayoutChooser.selectLayoutCandidate(dataflow, prunedSegments,
                digest);
        if (candidateAndInfluence == null && QueryContext.current().isPartialMatchIndex()) {
            // This branch is customized requirements
            log.trace("Partial dataflow matching");
            candidateAndInfluence = QueryLayoutChooser.selectPartialLayoutCandidate(dataflow, prunedSegments, digest);
        } else if (candidateAndInfluence == null) {
            log.debug("select the layout candidate with high data integrity.");
            candidateAndInfluence = QueryLayoutChooser.selectHighIntegrityCandidate(dataflow, candidate, digest);
            if (candidateAndInfluence != null) {
                result.setPartialResult(true);
            }
        }
        if (candidateAndInfluence != null) {
            chosenCandidate = candidateAndInfluence;
            result.influences.addAll(candidateAndInfluence.getCapabilityResult().influences);
            log.info("Matched layout {} snapshot in dataflow {} ", chosenCandidate, dataflow);
        }

        if (chosenCandidate != null) {
            result.setCapable(true);
            result.setCandidate(dataflow.isStreaming(), chosenCandidate);
            result.setCost(chosenCandidate.getCost());
        } else {
            result.setCapable(false);
        }
        return result;
    }

    public static CapabilityResult hybridRealizationCheck(HybridRealization r, Candidate candidate, SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();

        resolveSegmentsOverlap(r, candidate.getQueryableSeg().getStreamingSegments());
        for (IRealization realization : r.getRealizations()) {
            NDataflow df = (NDataflow) realization;
            CapabilityResult child = DataflowCapabilityChecker.check(df, candidate, digest);
            result.setCandidate(df.isStreaming(), child);
            if (child.isCapable()) {
                result.setCost(Math.min(result.getCost(), child.getCost(df.isStreaming())));
                result.setCapable(true);
                result.influences.addAll(child.influences);
            } else {
                result.incapableCause = child.incapableCause;
            }
        }

        result.setCost(result.getCost() - 1); // let hybrid win its children

        return result;
    }

    // Use batch segment when there's overlap of batch and stream segments, like follows
    // batch segments:seg1['2012-01-01', '2012-02-01'], seg2['2012-02-01', '2012-03-01'],
    // stream segments:seg3['2012-02-01', '2012-03-01'], seg4['2012-03-01', '2012-04-01']
    // the chosen segments is: [seg1, seg2, seg4]
    private static void resolveSegmentsOverlap(HybridRealization realization,
            List<NDataSegment> prunedStreamingSegments) {
        long end = realization.getBatchRealization().getDateRangeEnd();
        if (end != Long.MIN_VALUE) {
            String segments = prunedStreamingSegments.toString();
            log.info("Before resolve segments overlap between batch and stream of fusion model: {}", segments);
            SegmentRange.BasicSegmentRange range = new SegmentRange.KafkaOffsetPartitionedSegmentRange(end,
                    Long.MAX_VALUE);
            List<NDataSegment> list = ((NDataflow) realization.getStreamingRealization())
                    .getQueryableSegmentsByRange(range);
            prunedStreamingSegments.removeIf(seg -> !list.contains(seg));
            segments = prunedStreamingSegments.toString();
            log.info("After resolve segments overlap between batch and stream of fusion model: {}", segments);
        }
    }
}
