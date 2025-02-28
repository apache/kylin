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

package org.apache.kylin.query.util;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.relnode.OlapJoinRel;
import org.apache.kylin.query.relnode.OlapRel;

public class RelAggPushDownUtil {

    private RelAggPushDownUtil() {
    }

    private static final Pattern CTX_REMOVER = Pattern.compile(", ctx=\\[(.*?)]");

    public static void clearUnmatchedJoinDigest() {
        QueryContext.current().getUnmatchedJoinDigest().clear();
    }

    public static boolean canRelAnsweredBySnapshot(String project, RelNode relNode) {
        List<RelOptTable> rightRelOptTableList = RelOptUtil.findAllTables(relNode);
        if (rightRelOptTableList.size() != 1) {
            return false;
        }

        RelOptTable relOptTable = rightRelOptTableList.get(0);
        List<String> qualifiedNameList = relOptTable.getQualifiedName();
        String identity = StringUtils.join(qualifiedNameList, ".");
        TableDesc table = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getTableDesc(identity);
        return table != null && StringUtils.isNotEmpty(table.getLastSnapshotPath());
    }

    public static void registerUnmatchedJoinDigest(RelNode relNode) {
        OlapJoinRel joinRel = collectFirstJoinRel(relNode);
        if (joinRel != null) {
            QueryContext.current().getUnmatchedJoinDigest().put(trimExplainInfo(joinRel), true);
        }
    }

    // Each time a single join rel node push down， delete other join rel node
    public static synchronized boolean isUnmatchedJoinRel(OlapJoinRel joinRel) {
        boolean unmatched = QueryContext.current().getUnmatchedJoinDigest().get(trimExplainInfo(joinRel)) != null;
        if (unmatched) {
            QueryContext.current().getUnmatchedJoinDigest().clear();
            QueryContext.current().getUnmatchedJoinDigest().put(trimExplainInfo(joinRel), true);
        }
        return unmatched;
    }

    private static String trimExplainInfo(OlapJoinRel rel) {
        return CTX_REMOVER.matcher(rel.explain()).replaceAll("");
    }

    public static OlapJoinRel collectFirstJoinRel(RelNode relNode) {
        if (relNode == null || CollectionUtils.isEmpty(relNode.getInputs())) {
            return null;
        }
        if (relNode instanceof OlapJoinRel) {
            return (OlapJoinRel) relNode;
        } else {
            return collectFirstJoinRel(relNode.getInput(0));
        }
    }

    // only for test
    public static void collectAllJoinRel(RelNode relNode) {
        if (relNode instanceof OlapJoinRel) {
            QueryContext.current().getUnmatchedJoinDigest().put(trimExplainInfo((OlapJoinRel) relNode), true);
        }
        for (RelNode rel : relNode.getInputs()) {
            collectAllJoinRel(rel);
        }
    }

    public static void clearCtxRelNode(RelNode relNode) {
        List<RelNode> relNodes = relNode.getInputs();
        for (RelNode childNode : relNodes) {
            OlapRel olapRel = ((OlapRel) childNode);
            if (olapRel.getContext() != null) {
                olapRel.setContext(null);
            }
            clearCtxRelNode(childNode);
        }
    }
}
