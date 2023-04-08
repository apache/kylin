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

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.cuboid.AggIndexMatcher;
import org.apache.kylin.metadata.cube.cuboid.ChooserContext;
import org.apache.kylin.metadata.cube.cuboid.IndexMatcher;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.cuboid.TableIndexMatcher;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.AntiFlatChecker;
import org.apache.kylin.metadata.model.ColExcludedChecker;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.util.QueryInterruptChecker;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VacantIndexPruningRule extends PruningRule {
    @Override
    public void apply(Candidate candidate) {
        if (nonBatchRealizationSkipEmptySegments(candidate)) {
            log.info("{}({}/{}): only batch model support this feature, but the type of this model is {}",
                    this.getClass().getName(), candidate.getRealization().getProject(),
                    candidate.getRealization().getCanonicalName(),
                    candidate.getRealization().getModel().getModelType());
            return;
        }

        if (candidate.getCapability() == null || candidate.getCapability().isCapable()) {
            log.debug("skip the rule of {}.", this.getClass().getName());
            return;
        }

        List<IRealization> realizations = candidate.getRealization().getRealizations();
        if (CollectionUtils.isEmpty(realizations)) {
            log.warn("It seems that unlikely things happened when matching indexes haven't built. "
                    + "Expected size of realizations(models) is 1.");
            return;
        }
        NDataflow dataflow = (NDataflow) realizations.get(0);
        CapabilityResult capabilityResult = match(dataflow, candidate.getCtx().getSQLDigest());
        candidate.setCapability(capabilityResult);
    }

    private CapabilityResult match(NDataflow dataflow, SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();
        log.info("Try matching no built indexes from model.");

        NLayoutCandidate layoutCandidate = selectLayoutCandidate(dataflow, digest);
        if (layoutCandidate != null) {
            result.influences.addAll(layoutCandidate.getCapabilityResult().influences);
            result.setLayoutUnmatchedColsSize(layoutCandidate.getCapabilityResult().getLayoutUnmatchedColsSize());
            result.setSelectedCandidate(layoutCandidate);
            log.info("Matched layout {} snapshot in dataflow {} ", layoutCandidate, dataflow);
            result.setCapable(true);
            result.setVacant(true);
        } else {
            result.setCapable(false);
        }

        return result;
    }

    private NLayoutCandidate selectLayoutCandidate(NDataflow dataflow, SQLDigest sqlDigest) {

        // This section is same as NQueryLayoutChooser#selectLayoutCandidate.
        String project = dataflow.getProject();
        NDataModel model = dataflow.getModel();
        KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
        ChooserContext chooserContext = new ChooserContext(sqlDigest, dataflow);
        ColExcludedChecker excludedChecker = new ColExcludedChecker(projectConfig, project, model);
        AntiFlatChecker antiFlatChecker = new AntiFlatChecker(model.getJoinTables(), model);
        AggIndexMatcher aggIndexMatcher = new AggIndexMatcher(sqlDigest, chooserContext, dataflow, excludedChecker,
                antiFlatChecker);
        TableIndexMatcher tableIndexMatcher = new TableIndexMatcher(sqlDigest, chooserContext, dataflow,
                excludedChecker, antiFlatChecker);

        if (chooserContext.isIndexMatchersInvalid()) {
            return null;
        }

        // Find a layout that can match the sqlDigest, even if it hasn't been built yet.
        NLayoutCandidate candidate = findCandidate(dataflow, aggIndexMatcher, tableIndexMatcher);
        QueryInterruptChecker.checkThreadInterrupted("Interrupted exception occurs.",
                "Current step were matching indexes haven't built ");
        return candidate;
    }

    private static NLayoutCandidate findCandidate(NDataflow dataflow, AggIndexMatcher aggIndexMatcher,
            TableIndexMatcher tableIndexMatcher) {
        List<LayoutEntity> allLayouts = dataflow.getIndexPlan().getAllLayouts();
        for (LayoutEntity layout : allLayouts) {
            NLayoutCandidate candidate = new NLayoutCandidate(layout);
            IndexMatcher.MatchResult matchResult = tableIndexMatcher.match(layout);
            if (!matchResult.isMatched()) {
                matchResult = aggIndexMatcher.match(layout);
            }

            if (!matchResult.isMatched()) {
                continue;
            }

            CapabilityResult tempResult = new CapabilityResult();
            tempResult.setSelectedCandidate(candidate);
            candidate.setCapabilityResult(tempResult);
            return candidate;
        }
        return null;
    }

    private boolean nonBatchRealizationSkipEmptySegments(Candidate candidate) {
        IRealization realization = candidate.getRealization();
        return realization instanceof HybridRealization || realization.isStreaming();
    }
}
