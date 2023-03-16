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

package org.apache.kylin.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.apache.kylin.query.util.QueryContextCutter;

import com.clearspring.analytics.util.Lists;

public class OlapContextUtil {

    public static List<OLAPContext> getOlapContexts(String project, String sql) throws SqlParseException {
        return getOlapContexts(project, sql, false);
    }

    public static List<OLAPContext> getOlapContexts(String project, String sql, boolean reCutBanned)
            throws SqlParseException {
        QueryExec queryExec = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        RelNode rel = queryExec.parseAndOptimize(sql);
        try {
            QueryContextCutter.selectRealization(rel, reCutBanned);
        } catch (NoRealizationFoundException e) {
            // When NoRealizationFoundException occurs, do nothing
            // because we only need to obtain OlapContexts.
        }

        return getOlapContexts();
    }

    public static List<OLAPContext> getHepRulesOptimizedOlapContexts(String project, String sql, boolean reCutBanned)
            throws SqlParseException {
        QueryExec queryExec = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        RelNode rel = queryExec.parseAndOptimize(sql);
        List<OLAPContext> olapContexts = Lists.newArrayList();
        try {
            List<RelNode> relNodes = queryExec.postOptimize(rel);
            relNodes.forEach(relNode -> {
                QueryContextCutter.selectRealization(relNode, reCutBanned);
                olapContexts.addAll(getOlapContexts());
            });
        } catch (NoRealizationFoundException e) {
            // When NoRealizationFoundException occurs, do nothing
            // because we only need to obtain OlapContexts.
        }

        return olapContexts;
    }

    private static List<OLAPContext> getOlapContexts() {
        List<OLAPContext> result = Lists.newArrayList();
        Collection<OLAPContext> contexts = OLAPContext.getThreadLocalContexts();
        if (contexts != null) {
            result.addAll(contexts);
            result.forEach(olap -> {
                if (olap.isFixedModel()) {
                    olap.unfixModel();
                }
            });
        }
        return result;
    }

    public static Map<String, String> matchJoins(NDataModel model, OLAPContext ctx) {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(ctx.olapSchema.getProjectName());
        boolean isPartialInnerJoin = projectConfig.isQueryMatchPartialInnerJoinModel();
        boolean isPartialNonEquiJoin = projectConfig.partialMatchNonEquiJoins();
        return RealizationChooser.matchJoins(model, ctx, isPartialInnerJoin, isPartialNonEquiJoin);
    }
}
