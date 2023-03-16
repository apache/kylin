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
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPContextProp;
import org.apache.kylin.query.routing.rules.PartitionPruningRule;
import org.apache.kylin.query.routing.rules.RealizationSortRule;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.query.routing.rules.RemoveUncapableRealizationsRule;
import org.apache.kylin.query.routing.rules.SegmentPruningRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

/**
 * @author xjiang
 */
public class QueryRouter {

    private static final Logger logger = LoggerFactory.getLogger(QueryRouter.class);

    private QueryRouter() {
    }

    private static final List<RoutingRule> LAYOUT_CHOOSING_RULES = Lists.newLinkedList();

    static {
        LAYOUT_CHOOSING_RULES.add(new RemoveBlackoutRealizationsRule());
        LAYOUT_CHOOSING_RULES.add(new SegmentPruningRule());
        LAYOUT_CHOOSING_RULES.add(new PartitionPruningRule());
        LAYOUT_CHOOSING_RULES.add(new RemoveUncapableRealizationsRule());
        LAYOUT_CHOOSING_RULES.add(new RealizationSortRule());
    }

    public static void applyRules(List<Candidate> candidates) {
        for (RoutingRule rule : LAYOUT_CHOOSING_RULES) {
            String before = getPrintableText(candidates);
            rule.apply(candidates);
            String after = getPrintableText(candidates);
            if (!before.equals(after)) {
                logger.info("Applying rule: {}, realizations before:{}, realizations after: {}", rule, before, after);
            }
        }
    }

    public static String getPrintableText(List<Candidate> candidates) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Candidate candidate : candidates) {
            IRealization r = candidate.realization;
            sb.append(r.getCanonicalName());
            sb.append(",");
        }
        if (sb.charAt(sb.length() - 1) != '[')
            sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }

    public static void removeRule(RoutingRule rule) {
        LAYOUT_CHOOSING_RULES.removeIf(r -> r.getClass() == rule.getClass());
    }

    public static Candidate selectRealization(OLAPContext olapContext, IRealization realization,
            Map<String, String> aliasMap) {
        if (!realization.isReady()) {
            logger.warn("Realization {} is not ready", realization);
            return null;
        }

        List<Candidate> candidates = Lists.newArrayListWithCapacity(1);
        candidates.add(new Candidate(realization, olapContext, aliasMap));
        logger.info("Find candidates by table {} and project={} : {}", olapContext.firstTableScan.getTableName(),
                olapContext.olapSchema.getProjectName(), StringUtils.join(candidates, ","));
        List<Candidate> originCandidates = Lists.newArrayList(candidates);

        // rule based realization selection, rules might reorder realizations or remove specific realization
        QueryRouter.applyRules(candidates);

        collectIncapableReason(olapContext, originCandidates);
        if (candidates.isEmpty()) {
            return null;
        }

        Candidate chosen = candidates.get(0);
        chosen.setRewrittenCtx(preserveRewriteProps(olapContext));
        logger.info("The realizations remaining: {}, and the final chosen one for current olap context {} is {}",
                QueryRouter.getPrintableText(candidates), olapContext.id, chosen.realization.getCanonicalName());
        return chosen;
    }

    static OLAPContextProp preserveRewriteProps(OLAPContext rewrittenOLAContext) {
        return preservePropsBeforeRewrite(rewrittenOLAContext);
    }

    static OLAPContextProp preservePropsBeforeRewrite(OLAPContext oriOLAPContext) {
        OLAPContextProp preserved = new OLAPContextProp(-1);
        preserved.allColumns = Sets.newHashSet(oriOLAPContext.allColumns);
        preserved.setSortColumns(Lists.newArrayList(oriOLAPContext.getSortColumns()));
        preserved.setInnerGroupByColumns(Sets.newHashSet(oriOLAPContext.getInnerGroupByColumns()));
        preserved.setGroupByColumns(Sets.newLinkedHashSet(oriOLAPContext.getGroupByColumns()));
        preserved.setInnerFilterColumns(Sets.newHashSet(oriOLAPContext.getInnerFilterColumns()));
        for (FunctionDesc agg : oriOLAPContext.aggregations) {
            preserved.getReservedMap().put(agg,
                    FunctionDesc.newInstance(agg.getExpression(), agg.getParameters(), agg.getReturnType()));
        }

        return preserved;
    }

    static void restoreOLAPContextProps(OLAPContext oriOLAPContext, OLAPContextProp preservedOLAPContext) {
        oriOLAPContext.allColumns = preservedOLAPContext.allColumns;
        oriOLAPContext.setSortColumns(preservedOLAPContext.getSortColumns());
        oriOLAPContext.aggregations.forEach(agg -> {
            if (preservedOLAPContext.getReservedMap().containsKey(agg)) {
                final FunctionDesc functionDesc = preservedOLAPContext.getReservedMap().get(agg);
                agg.setExpression(functionDesc.getExpression());
                agg.setParameters(functionDesc.getParameters());
                agg.setReturnType(functionDesc.getReturnType());
            }
        });
        oriOLAPContext.setGroupByColumns(preservedOLAPContext.getGroupByColumns());
        oriOLAPContext.setInnerGroupByColumns(preservedOLAPContext.getInnerGroupByColumns());
        oriOLAPContext.setInnerFilterColumns(preservedOLAPContext.getInnerFilterColumns());
        oriOLAPContext.resetSQLDigest();
    }

    private static void collectIncapableReason(OLAPContext olapContext, List<Candidate> candidates) {
        for (Candidate candidate : candidates) {
            if (!candidate.getCapability().capable) {
                RealizationCheck.IncapableReason reason = RealizationCheck.IncapableReason
                        .create(candidate.getCapability().incapableCause);
                if (reason != null)
                    olapContext.realizationCheck.addIncapableCube(candidate.getRealization(), reason);
            } else {
                olapContext.realizationCheck.addCapableCube(candidate.getRealization());
            }
        }
    }
}
