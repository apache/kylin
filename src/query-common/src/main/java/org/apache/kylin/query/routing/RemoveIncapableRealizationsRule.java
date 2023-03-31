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
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.query.util.ComputedColumnRewriter;
import org.apache.kylin.query.util.QueryAliasMatchInfo;

import lombok.extern.slf4j.Slf4j;

/**
 */
@Slf4j
public class RemoveIncapableRealizationsRule extends RoutingRule {
    @Override
    public void apply(Candidate candidate) {
        if (candidate.getCapability() != null) {
            return;
        }
        candidate.getCtx().resetSQLDigest();
        CapabilityResult capability = candidate.getRealization().isCapable(candidate.getCtx().getSQLDigest(),
                candidate.getQueryableSeg());

        if (!capability.isCapable() && !candidate.getRealization().getModel().getComputedColumnDescs().isEmpty()) {
            log.info("{}({}/{}): try rewrite computed column and then check whether the realization is capable.",
                    this.getClass().getName(), candidate.getRealization().getProject(),
                    candidate.getRealization().getCanonicalName());
            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.putAll(candidate.getMatchedJoinsGraphAliasMap());
            ComputedColumnRewriter.rewriteCcInnerCol(candidate.getCtx(), candidate.getRealization().getModel(),
                    new QueryAliasMatchInfo(aliasMapping, null));
            candidate.getCtx().resetSQLDigest();
            capability = candidate.getRealization().isCapable(candidate.getCtx().getSQLDigest(),
                    candidate.getQueryableSeg());
        }

        candidate.setCapability(capability);
    }

}
