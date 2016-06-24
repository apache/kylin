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
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.realization.CapabilityResult.DimensionAsMeasure;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * @author xjiang
 */
public class QueryRouter {

    private static final Logger logger = LoggerFactory.getLogger(QueryRouter.class);

    public static IRealization selectRealization(OLAPContext olapContext) throws NoRealizationFoundException {

        ProjectManager prjMgr = ProjectManager.getInstance(olapContext.olapSchema.getConfig());
        logger.info("The project manager's reference is " + prjMgr);
        String factTableName = olapContext.firstTableScan.getTableName();
        String projectName = olapContext.olapSchema.getProjectName();
        Set<IRealization> realizations = prjMgr.getRealizationsByTable(projectName, factTableName);
        SQLDigest sqlDigest = olapContext.getSQLDigest();

        List<Candidate> candidates = Lists.newArrayListWithCapacity(realizations.size());
        for (IRealization real : realizations) {
            if (real.isReady())
                candidates.add(new Candidate(real, sqlDigest));
        }

        logger.info("Find candidates by table " + factTableName + " and project=" + projectName + " : " + StringUtils.join(candidates, ","));

        // rule based realization selection, rules might reorder realizations or remove specific realization
        RoutingRule.applyRules(candidates);

        if (candidates.size() == 0) {
            throw new NoRealizationFoundException("Can't find any realization. Please confirm with providers. SQL digest: " + sqlDigest.toString());
        }

        Candidate chosen = candidates.get(0);
        adjustForDimensionAsMeasure(chosen, olapContext);

        logger.info("The realizations remaining: " + RoutingRule.getPrintableText(candidates) + " And the final chosen one is the first one");

        return chosen.realization;
    }

    private static void adjustForDimensionAsMeasure(Candidate chosen, OLAPContext olapContext) {
        CapabilityResult capability = chosen.getCapability();
        for (CapabilityInfluence inf : capability.influences) {
            // convert the metric to dimension
            if (inf instanceof DimensionAsMeasure) {
                FunctionDesc functionDesc = ((DimensionAsMeasure) inf).getMeasureFunction();
                functionDesc.setDimensionAsMetric(true);
                logger.info("Adjust DimensionAsMeasure for " + functionDesc);
            }
        }
    }

}
