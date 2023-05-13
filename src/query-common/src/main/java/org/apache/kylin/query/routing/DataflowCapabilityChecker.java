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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;
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
        if (digest.limitPrecedesAggr) {
            log.info("Exclude NDataflow {} because there's limit preceding aggregation", dataflow);
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.LIMIT_PRECEDE_AGGR);
            return result;
        }

        // 1. match joins is ensured at model select
        String factTableOfQuery = digest.factTable;
        String modelFactTable = dataflow.getModel().getQueryCompatibleFactTable(factTableOfQuery);
        IRealizationCandidate chosenCandidate = null;
        if (digest.joinDescs.isEmpty() && !modelFactTable.equals(factTableOfQuery)) {
            log.trace("Snapshot dataflow matching");
            chosenCandidate = tryMatchLookup(dataflow, digest, result);
            if (chosenCandidate != null) {
                log.info("Matched table {} snapshot in dataflow {} ", factTableOfQuery, dataflow);
            }
        } else {
            // for query-on-fact-table
            log.trace("Normal dataflow matching");
            List<NDataSegment> prunedSegments = candidate.getPrunedSegments(dataflow);
            Map<String, Set<Long>> secondStorageSegmentLayoutMap = candidate.getChSegToLayoutsMap(dataflow);
            NLayoutCandidate candidateAndInfluence = QueryLayoutChooser.selectLayoutCandidate(dataflow, prunedSegments,
                    digest, secondStorageSegmentLayoutMap);
            if (candidateAndInfluence == null && QueryContext.current().isPartialMatchIndex()) {
                // This branch is customized requirements
                log.trace("Partial dataflow matching");
                candidateAndInfluence = QueryLayoutChooser.selectPartialLayoutCandidate(dataflow, prunedSegments,
                        digest, secondStorageSegmentLayoutMap);
            } else if (candidateAndInfluence == null) {
                log.debug("select the layout candidate with high data integrity.");
                candidateAndInfluence = QueryLayoutChooser.selectHighIntegrityCandidate(dataflow, prunedSegments,
                        digest);
                if (candidateAndInfluence != null) {
                    result.setPartialResult(true);
                }
            }
            if (candidateAndInfluence != null) {
                chosenCandidate = candidateAndInfluence;
                result.influences.addAll(candidateAndInfluence.getCapabilityResult().influences);
                log.info("Matched layout {} snapshot in dataflow {} ", chosenCandidate, dataflow);
            }
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

    private static IRealizationCandidate tryMatchLookup(NDataflow dataflow, SQLDigest digest, CapabilityResult result) {
        // query from snapShot table
        NTableMetadataManager nTableMetadataManager = NTableMetadataManager.getInstance(dataflow.getConfig(),
                dataflow.getProject());
        if (dataflow.getLatestReadySegment() == null)
            return null;

        if (StringUtils.isEmpty(nTableMetadataManager.getTableDesc(digest.factTable).getLastSnapshotPath())) {
            log.info("Exclude NDataflow {} because snapshot of table {} does not exist", dataflow, digest.factTable);
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.NOT_EXIST_SNAPSHOT);
            result.setCapable(false);
            return null;
        }

        //1. all aggregations on lookup table can be done
        NDataModel dataModel = dataflow.getModel();
        if (dataModel.isFusionModel()) {
            dataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), dataflow.getProject())
                    .getDataModelDesc(dataflow.getModel().getFusionId());
        }
        Set<TblColRef> colsOfSnapShot = Sets.newHashSet(dataModel.findFirstTable(digest.factTable).getColumns());
        Collection<TblColRef> unmatchedCols = Sets.newHashSet(digest.allColumns);
        if (!unmatchedCols.isEmpty()) {
            unmatchedCols.removeAll(colsOfSnapShot);
        }

        if (!unmatchedCols.isEmpty()) {
            log.info("Exclude NDataflow {} because unmatched dimensions [{}] in Snapshot", dataflow, unmatchedCols);
            result.incapableCause = CapabilityResult.IncapableCause.unmatchedDimensions(unmatchedCols);
            return null;
        } else {
            return new NLookupCandidate(digest.factTable, true);
        }
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
