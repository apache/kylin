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

import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.HashBiMap;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OlapContextProp;
import org.apache.kylin.query.util.ComputedColumnRewriter;
import org.apache.kylin.query.util.QueryAliasMatchInfo;

import lombok.extern.slf4j.Slf4j;

/**
 */
@Slf4j
public class RemoveIncapableRealizationsRule extends PruningRule {
    @Override
    public void apply(Candidate candidate) {
        if (!isStorageMatch(candidate)) {
            return;
        }

        if (candidate.getCapability() != null) {
            return;
        }

        // Preserve the initial OlapContext and initialize the matching result of Candidate as false.
        OlapContextProp propsBeforeRewrite = RealizationChooser.preservePropsBeforeRewrite(candidate.getCtx());
        CapabilityResult capabilityResult = new CapabilityResult();

        IRealization realization = candidate.getRealization();
        if (!realization.getModel().getComputedColumnDescs().isEmpty()) {
            BiMap<String, String> aliasMapping = HashBiMap.create(candidate.getMatchedJoinsGraphAliasMap());
            ComputedColumnRewriter.rewriteCcInnerCol(candidate.getCtx(), realization.getModel(),
                    new QueryAliasMatchInfo(aliasMapping, null));
            candidate.getCtx().resetSQLDigest();
            capabilityResult = getCapabilityResult(candidate);
            candidate.recordRewrittenCtxProps();
        }

        if (!capabilityResult.isCapable()) {
            RealizationChooser.restoreOlapContextProps(candidate.getCtx(), propsBeforeRewrite);
            candidate.getCtx().resetSQLDigest();
            capabilityResult = getCapabilityResult(candidate);
            candidate.recordRewrittenCtxProps();
        }

        candidate.setCapability(capabilityResult);
    }

    @Override
    public boolean isStorageMatch(Candidate candidate) {
        return candidate.getRealization().getModel().getStorageType().isV1Storage();
    }

    private CapabilityResult getCapabilityResult(Candidate candidate) {
        IRealization realization = candidate.getRealization();
        SQLDigest sqlDigest = candidate.getCtx().getSQLDigest();
        CapabilityResult capability;
        if (realization instanceof HybridRealization) {
            capability = DataflowCapabilityChecker.hybridRealizationCheck((HybridRealization) realization, candidate,
                    sqlDigest);
        } else {
            capability = DataflowCapabilityChecker.check((NDataflow) realization, candidate, sqlDigest);
        }

        // The matching process may modify the dimensions and measures info of the  OlapContext, 
        // so we need these properties to be recorded in the candidate's rewrittenCtx. It is important
        // that once the OlapContext matched an index, no further matching will be performed.
        candidate.recordRewrittenCtxProps();
        return capability;
    }
}
