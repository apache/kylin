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

package org.apache.kylin.metadata.cube.model;

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
import org.apache.kylin.metadata.cube.cuboid.NQueryLayoutChooser;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealizationCandidate;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NDataflowCapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(NDataflowCapabilityChecker.class);

    public static CapabilityResult check(NDataflow dataflow, List<NDataSegment> prunedSegments, SQLDigest digest,
            Map<String, Set<Long>> secondStorageSegmentLayoutMap) {
        logger.info("Matching Layout in dataflow {}, SQL digest {}", dataflow, digest);
        CapabilityResult result = new CapabilityResult();
        if (digest.limitPrecedesAggr) {
            logger.info("Exclude NDataflow {} because there's limit preceding aggregation", dataflow);
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.LIMIT_PRECEDE_AGGR);
            return result;
        }

        // 1. match joins is ensured at model select
        String rootFactTable = dataflow.getModel().getRootFactTableName();
        NDataModel model = dataflow.getModel();
        if (!rootFactTable.equals(digest.factTable) && model.isFusionModel() && !dataflow.isStreaming()) {
            NDataModel streamingModel = NDataModelManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), dataflow.getProject())
                    .getDataModelDesc(model.getFusionId());
            rootFactTable = streamingModel.getRootFactTableName();
        }
        IRealizationCandidate chosenCandidate = null;
        if (digest.joinDescs.isEmpty() && !rootFactTable.equals(digest.factTable)) {
            logger.trace("Snapshot dataflow matching");
            chosenCandidate = tryMatchLookup(dataflow, digest, result);
            if (chosenCandidate != null) {
                logger.info("Matched table {} snapshot in dataflow {} ", digest.factTable, dataflow);
            }
        } else {
            // for query-on-fact-table
            logger.trace("Normal dataflow matching");
            boolean partialMatchIndex = QueryContext.current().isPartialMatchIndex();
            NLayoutCandidate candidateAndInfluence = NQueryLayoutChooser.selectLayoutCandidate(dataflow, prunedSegments,
                    digest, secondStorageSegmentLayoutMap);
            if (partialMatchIndex && candidateAndInfluence == null) {
                logger.trace("Partial dataflow matching");
                candidateAndInfluence = NQueryLayoutChooser.selectPartialLayoutCandidate(dataflow, prunedSegments,
                        digest, secondStorageSegmentLayoutMap);
            }
            if (candidateAndInfluence != null) {
                chosenCandidate = candidateAndInfluence;
                result.influences.addAll(candidateAndInfluence.getCapabilityResult().influences);
                logger.info("Matched layout {} snapshot in dataflow {} ", chosenCandidate, dataflow);
            }
        }
        if (chosenCandidate != null) {
            result.setCapable(true);
            if (dataflow.isStreaming()) {
                result.setSelectedStreamingCandidate(chosenCandidate);
            } else {
                result.setSelectedCandidate(chosenCandidate);
            }
            result.cost = (int) chosenCandidate.getCost();
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
            logger.info("Exclude NDataflow {} because snapshot of table {} does not exist", dataflow, digest.factTable);
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
            logger.info("Exclude NDataflow {} because unmatched dimensions [{}] in Snapshot", dataflow, unmatchedCols);
            result.incapableCause = CapabilityResult.IncapableCause.unmatchedDimensions(unmatchedCols);
            return null;
        } else {
            return new NLookupCandidate(digest.factTable, true);
        }
    }
}
