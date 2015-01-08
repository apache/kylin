/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.query.routing;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.kylinolap.metadata.project.ProjectManager;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.query.relnode.OLAPContext;

/**
 * @author xjiang
 */
public class QueryRouter {

    private static final Logger logger = LoggerFactory.getLogger(QueryRouter.class);

    public static IRealization selectRealization(OLAPContext olapContext) throws NoRealizationFoundException {

        ProjectManager prjMgr = ProjectManager.getInstance(olapContext.olapSchema.getConfig());
        String factTableName = olapContext.firstTableScan.getTableName();
        String projectName = olapContext.olapSchema.getProjectName();
        List<IRealization> realizations = Lists.newArrayList(prjMgr.getRealizationsByTable(projectName, factTableName));

        //rule based realization selection, rules might reorder realizations or remove specific realization
        RoutingRule.applyRules(realizations, olapContext);

        if (realizations.size() == 0) {
            throw new NoRealizationFoundException("Can't find any realization for fact table " + olapContext.firstTableScan.getTableName() //
                    + " in project " + olapContext.olapSchema.getProjectName() + " with dimensions " //
                    + olapContext.getDimensionColumns() + " and measures " + olapContext.aggregations //
                    + ". Also please check whether join types match what defined in realization.");
        }

        logger.info("The realizations remaining: ");
        logger.info(RoutingRule.getPrintableText(realizations));
        logger.info("The realization being chosen: " + realizations.get(0).getName());

        return realizations.get(0);
    }
}
