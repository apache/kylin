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
import java.util.function.Consumer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.HashBiMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.NoStreamingRealizationFoundException;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.ComputedColumnRewriter;
import org.apache.kylin.query.util.QueryAliasMatchInfo;
import org.apache.kylin.query.util.QueryContextCutter;

public class OlapContextTestUtil {

    public static List<OlapContext> getOlapContexts(String project, String sql) throws SqlParseException {
        return getOlapContexts(project, sql, false);
    }

    public static List<OlapContext> getOlapContexts(String project, String sql, boolean reCutBanned)
            throws SqlParseException {
        return getOlapContexts(project, sql, reCutBanned, false);
    }

    public static List<OlapContext> getOlapContexts(String project, String sql, boolean reCutBanned,
            boolean postOptimize) throws SqlParseException {
        QueryExec queryExec = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        try {
            RelNode rel = queryExec.parseAndOptimize(sql);
            if (postOptimize) {
                List<RelNode> relNodes = queryExec.postOptimize(rel);
                rel = relNodes.get(0);
            }
            QueryContextCutter.selectRealization(project, rel, reCutBanned);
        } catch (NoRealizationFoundException | NoStreamingRealizationFoundException e) {
            // When NoRealizationFoundException occurs, do nothing
            // because we only need to obtain OlapContexts.
        }

        return getOlapContexts();
    }

    public static RelNode cutOlapContextsAndReturnRelNode(String project, String sql) throws SqlParseException {
        QueryExec queryExec = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        RelNode rel = null;
        try {
            rel = queryExec.parseAndOptimize(sql);
            QueryContextCutter.selectRealization(project, rel, false);
        } catch (NoRealizationFoundException | NoStreamingRealizationFoundException e) {
            // When NoRealizationFoundException occurs, do nothing
            // because we only need to obtain OlapContexts.
        }
        return rel;
    }

    public static List<OlapContext> getOlapContexts(String project, String sql, boolean reCutBanned,
            Consumer<NoRealizationFoundException> consumer) throws SqlParseException {
        QueryExec queryExec = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        try {
            RelNode rel = queryExec.parseAndOptimize(sql);
            QueryContextCutter.selectRealization(project, rel, reCutBanned);
        } catch (NoRealizationFoundException e) {
            consumer.accept(e);
        }
        return getOlapContexts();
    }

    public static List<OlapContext> getHepRulesOptimizedOlapContexts(String project, String sql, boolean reCutBanned)
            throws SqlParseException {
        QueryExec queryExec = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        RelNode rel = queryExec.parseAndOptimize(sql);
        List<OlapContext> olapContexts = Lists.newArrayList();
        try {
            List<RelNode> relNodes = queryExec.postOptimize(rel);
            relNodes.forEach(relNode -> {
                QueryContextCutter.selectRealization(project, relNode, reCutBanned);
                olapContexts.addAll(getOlapContexts());
            });
        } catch (NoRealizationFoundException e) {
            // When NoRealizationFoundException occurs, do nothing
            // because we only need to obtain OlapContexts.
        }

        return olapContexts;
    }

    private static List<OlapContext> getOlapContexts() {
        List<OlapContext> result = Lists.newArrayList();
        Collection<OlapContext> contexts = ContextUtil.getThreadLocalContexts();
        result.addAll(contexts);
        result.forEach(olap -> {
            if (olap.isFixedModel()) {
                olap.unfixModel();
            }
        });
        return result;
    }

    public static Map<String, String> matchJoins(NDataModel model, OlapContext ctx) {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(ctx.getOlapSchema().getProject());
        boolean isPartialInnerJoin = projectConfig.isQueryMatchPartialInnerJoinModel();
        boolean isPartialNonEquiJoin = projectConfig.partialMatchNonEquiJoins();
        return ctx.matchJoins(model, isPartialInnerJoin, isPartialNonEquiJoin);
    }

    public static void rewriteComputedColumns(NDataModel model, OlapContext olapContext) {
        Map<String, String> aliasMapping = matchJoins(model, olapContext);
        BiMap<String, String> aliasBiMap = HashBiMap.create(aliasMapping);
        QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasBiMap, null);
        ComputedColumnRewriter.rewriteCcInnerCol(olapContext, model, matchInfo);
    }
}
