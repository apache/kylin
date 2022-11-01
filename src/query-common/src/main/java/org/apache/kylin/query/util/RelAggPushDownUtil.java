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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.relnode.KapJoinRel;
import org.apache.kylin.query.relnode.KapRel;

public class RelAggPushDownUtil {

    private static final String HEP_REL_VERTEX = "HepRelVertex#";
    private static final String REL = "rel#";
    private static final String KAP = ":Kap";
    private static final String CTX = "ctx=";

    private RelAggPushDownUtil() {
    }

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
        RelNode joinRel = collectFirstJoinRel(relNode);
        if (joinRel != null) {
            String digest = joinRel.getDigest();
            digest = handleDigest(digest);
            QueryContext.current().getUnmatchedJoinDigest().put(digest, true);
        }
    }

    // Each time a single join rel node push down， delete other join rel node
    public static synchronized boolean isUnmatchedJoinRel(KapJoinRel joinRel) {
        String digest = getDigest(joinRel);
        boolean unmatched = QueryContext.current().getUnmatchedJoinDigest().get(digest) != null;
        if (unmatched) {
            QueryContext.current().getUnmatchedJoinDigest().clear();
            QueryContext.current().getUnmatchedJoinDigest().put(digest, true);
        }
        return unmatched;
    }

    private static String getDigest(KapJoinRel joinRel) {
        Map<String, String> cacheDescMap = new HashMap<>();
        analysisRel(joinRel, cacheDescMap);
        String joinRelDigest = joinRel.getDigest();
        int times = 0;
        int maxReplaceSize = cacheDescMap.size();
        while (joinRelDigest.contains(RelAggPushDownUtil.HEP_REL_VERTEX) && times < maxReplaceSize) {
            Iterator<String> iterator = cacheDescMap.keySet().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                if (joinRelDigest.contains(key)) {
                    joinRelDigest = joinRelDigest.replace(key, cacheDescMap.get(key));
                    iterator.remove();
                }
            }
            times++;
        }
        return joinRelDigest;
    }

    private static void analysisRel(RelNode relNode, Map<String, String> cacheDescMap) {
        if (relNode instanceof HepRelVertex) {
            HepRelVertex hepRelVertex = (HepRelVertex) relNode;
            cacheDescMap.put(relNode.getDescription(), hepRelVertex.getCurrentRel().getDigest());
            relNode = hepRelVertex.getCurrentRel();
        }
        List<RelNode> childRelNodes = relNode.getInputs();
        for (RelNode childRelNode : childRelNodes) {
            analysisRel(childRelNode, cacheDescMap);
        }
    }

    public static KapJoinRel collectFirstJoinRel(RelNode kapRel) {
        if (kapRel == null || CollectionUtils.isEmpty(kapRel.getInputs())) {
            return null;
        }
        if (kapRel instanceof KapJoinRel) {
            return (KapJoinRel) kapRel;
        } else {
            return collectFirstJoinRel(kapRel.getInput(0));
        }
    }

    // only for test
    public static void collectAllJoinRel(RelNode kapRel) {
        if (kapRel instanceof KapJoinRel) {
            QueryContext.current().getUnmatchedJoinDigest().put(handleDigest(kapRel.getDigest()), true);
        }
        for (RelNode rel : kapRel.getInputs()) {
            collectAllJoinRel(rel);
        }
    }

    public static void clearCtxRelNode(RelNode relNode) {
        List<RelNode> relNodes = relNode.getInputs();
        for (RelNode childNode : relNodes) {
            KapRel kapRel = ((KapRel) childNode);
            if (kapRel.getContext() != null) {
                kapRel.setContext(null);
            }
            clearCtxRelNode(childNode);
        }
    }

    private static String handleDigest(String digest) {
        digest = clearDigestRelID(digest);
        digest = clearDigestCtx(digest);
        return digest;
    }

    private static String clearDigestRelID(String digest) {
        int start = digest.indexOf(REL);
        int end = digest.indexOf(KAP);
        if (start > 0 && end > start) {
            digest = digest.substring(0, start) + digest.substring(end + 1);
            if (digest.contains(REL)) {
                return clearDigestRelID(digest);
            }
        }
        return digest;
    }

    public static String clearDigestCtx(String digest) {
        String[] digestArray = digest.split(CTX);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < digestArray.length; i++) {
            String temp = digestArray[i];
            if (i > 0 && !temp.startsWith(")")) {
                temp = temp.substring(temp.indexOf(","));
            }
            builder.append(temp);
            if (i < (digestArray.length - 1)) {
                builder.append(CTX);
            }
        }
        return builder.toString();
    }

}
