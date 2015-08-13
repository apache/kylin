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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
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
        List<IRealization> realizations = Lists.newArrayList(prjMgr.getRealizationsByTable(projectName, factTableName));
        logger.info("Find candidates by table " + factTableName + " and project=" + projectName + " : " + StringUtils.join(realizations, ","));

        //rule based realization selection, rules might reorder realizations or remove specific realization
        RoutingRule.applyRules(realizations, olapContext);

        if (realizations.size() == 0) {
            throw new NoRealizationFoundException("Can't find any realization. Please confirm with providers. SQL digest: " + olapContext.getSQLDigest().toString());
        }

        logger.info("The realizations remaining: ");
        logger.info(RoutingRule.getPrintableText(realizations));
        logger.info("The realization being chosen: " + realizations.get(0).getName());

        return realizations.get(0);
    }
}
