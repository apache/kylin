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

import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_TABLE_NOT_SUPPORT_AUTO_MODELING;

import java.util.List;
import java.util.Locale;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.NoStreamingRealizationFoundException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.routing.RealizationChooser;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.KapRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryContextCutter {
    private static final int MAX_RETRY_TIMES_OF_CONTEXT_CUT = 10;

    private static final Logger logger = LoggerFactory.getLogger(QueryContextCutter.class);

    /**
     * For a parser tree of one query, there are 3 steps to get it matched with pre-calculated realizations
     *      1. first-round cut the tree off several parts correspond to OLAPContext, which make the part as big as possible
     *      2. collect contexts' info and choose lowest-cost realization according to the context's info. if there are contexts cannot match realizations, goto 3
     *      3. seek proper context-cut methods to ensure as many contexts as possible match a realization, then goto 2.
     *
     * @return The cut OLAPContext with selected realizations, which is a subset of OLAPContext.getThreadLocalContexts().
     */
    public static List<OLAPContext> selectRealization(RelNode root, boolean isForAutoModeling) {
        FirstRoundContextCutStrategy firstRoundStrategy = new FirstRoundContextCutStrategy();
        QueryReCutContextStrategy reCutContextStrategy = null;

        new QueryContextCutter(firstRoundStrategy).cutContext((KapRel) root.getInput(0), root);
        int retryCutTimes = 0;
        while (retryCutTimes++ < MAX_RETRY_TIMES_OF_CONTEXT_CUT) {
            try {
                return collectContextInfoAndSelectRealization(root);
            } catch (NoStreamingRealizationFoundException e) {
                if (isForAutoModeling) {
                    checkStreamingTableWithAutoModeling();
                } else {
                    throw e;
                }
            } catch (NoRealizationFoundException e) {
                if (isForAutoModeling) {
                    throw e;
                }

                int ctxSeq = reCutContextStrategy == null ? OLAPContext.getThreadLocalContexts().size()
                        : reCutContextStrategy.getRecutContextImplementor().getCtxSeq();
                reCutContextStrategy = new QueryReCutContextStrategy(
                        new ICutContextStrategy.CutContextImplementor(ctxSeq));
                for (OLAPContext context : ContextUtil.listContextsHavingScan()) {
                    if (context.isHasSelected() && context.realization == null
                            && (!context.isHasPreCalcJoin() || context.getModelAlias() != null)) {
                        throw e;
                    } else if (context.isHasSelected() && context.realization == null) {
                        new QueryContextCutter(reCutContextStrategy).cutContext(context.getTopNode(), root);
                        ContextUtil.setSubContexts(root.getInput(0));
                        continue;
                    } else if (context.realization != null) {
                        context.unfixModel();
                    }
                    context.clearCtxInfo();
                }
            }
        }

        ContextUtil.dumpCalcitePlan(
                "cannot find proper realizations After re-cut " + MAX_RETRY_TIMES_OF_CONTEXT_CUT + " times", root, log);
        logger.error("too many unmatched join in this query, please check it or create correspond realization");
        throw new NoRealizationFoundException(
                "too many unmatched join in this query, please check it or create correspond realization");
    }

    private static List<OLAPContext> collectContextInfoAndSelectRealization(RelNode queryRoot) {
        // post-order travel children
        OLAPRel.OLAPImplementor kapImplementor = new OLAPRel.OLAPImplementor();
        kapImplementor.visitChild(queryRoot.getInput(0), queryRoot);
        QueryContext.current().record("collect_olap_context_info");
        // identify model
        List<OLAPContext> contexts = ContextUtil.listContextsHavingScan();

        for (OLAPContext olapContext : contexts) {
            logger.info("Context for realization matching: {}", olapContext);
        }

        long selectLayoutStartTime = System.currentTimeMillis();
        if (contexts.size() > 1) {
            RealizationChooser.multiThreadSelectLayoutCandidate(contexts);
        } else {
            RealizationChooser.selectLayoutCandidate(contexts);
        }
        logger.info("select layout candidate for {} olapContext cost {} ms", contexts.size(),
                System.currentTimeMillis() - selectLayoutStartTime);
        QueryContext.current().record("end select realization");
        return contexts;
    }

    // ============================================================================

    private ICutContextStrategy strategy;

    private QueryContextCutter(ICutContextStrategy cutContextStrategy) {
        this.strategy = cutContextStrategy;
    }

    private void cutContext(OLAPRel rootOfSubCtxTree, RelNode queryRoot) {
        if (strategy.needCutOff(rootOfSubCtxTree)) {
            strategy.cutOffContext(rootOfSubCtxTree, queryRoot);
        }
        if (strategy instanceof FirstRoundContextCutStrategy) {
            ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER OLAPCONTEXT IS SET IN FIRST ROUND", queryRoot, log);
        } else {
            ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER OLAPCONTEXT IS RE-CUT OFF ", queryRoot, log);
        }
    }

    private static void checkStreamingTableWithAutoModeling() {
        for (OLAPContext context : ContextUtil.listContextsHavingScan()) {
            for (OLAPTableScan tableScan : context.allTableScans) {
                TableDesc tableDesc = tableScan.getTableRef().getTableDesc();
                if (ISourceAware.ID_STREAMING == tableDesc.getSourceType()
                        && tableDesc.getKafkaConfig().hasBatchTable()) {
                    throw new NoStreamingRealizationFoundException(STREAMING_TABLE_NOT_SUPPORT_AUTO_MODELING, String
                            .format(Locale.ROOT, MsgPicker.getMsg().getStreamingTableNotSupportAutoModeling()));
                }
            }
        }
        throw new NoRealizationFoundException("No realization found for auto modeling.");
    }
}
