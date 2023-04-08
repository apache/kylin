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
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.KapRel;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.routing.RealizationChooser;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryContextCutter {
    private static final int MAX_RETRY_TIMES_OF_CONTEXT_CUT = 10;

    private QueryContextCutter() {
    }

    /**
     * For a parser tree of one query, there are 3 steps to get it matched with pre-calculated realizations
     *      1. first-round cut the tree off several parts correspond to OLAPContext, which make the part as big as possible
     *      2. collect contexts' info and choose lowest-cost realization according to the context's info. if there are contexts cannot match realizations, goto 3
     *      3. seek proper context-cut methods to ensure as many contexts as possible match a realization, then goto 2.
     *
     * @return The cut OLAPContext with selected realizations, which is a subset of OLAPContext.getThreadLocalContexts().
     */
    public static List<OLAPContext> selectRealization(RelNode root, boolean isReCutBanned) {
        ContextInitialCutStrategy firstRoundStrategy = new ContextInitialCutStrategy();
        ContextReCutStrategy reCutStrategy = new ContextReCutStrategy();

        QueryContextCutter.cutContext(firstRoundStrategy, (KapRel) root.getInput(0), root);
        int retryCutTimes = 0;
        while (retryCutTimes++ < MAX_RETRY_TIMES_OF_CONTEXT_CUT) {
            try {
                return collectContextInfoAndSelectRealization(root);
            } catch (NoRealizationFoundException | NoStreamingRealizationFoundException e) {
                if (isReCutBanned && e instanceof NoStreamingRealizationFoundException) {
                    checkStreamingTableWithAutoModeling();
                } else if (isReCutBanned) {
                    throw e;
                }
                reCutStrategy.tryCutToSmallerContexts(root, e);
            } finally {
                // auto-modeling should invoke unfixModel() because it may select some realizations.
                if (isReCutBanned) {
                    ContextUtil.listContextsHavingScan().forEach(olapContext -> {
                        if (olapContext.realization != null) {
                            olapContext.unfixModel();
                        }
                    });
                }
            }
        }

        String errorMsg = "too many unmatched joins in this query, please check it or create corresponding realization.";
        ContextUtil.dumpCalcitePlan("cannot find proper realizations After re-cut " + MAX_RETRY_TIMES_OF_CONTEXT_CUT
                + " times. \nError: " + errorMsg, root, log);
        throw new NoRealizationFoundException(errorMsg);
    }

    private static List<OLAPContext> collectContextInfoAndSelectRealization(RelNode queryRoot) {
        // post-order travel children
        OLAPRel.OLAPImplementor kapImplementor = new OLAPRel.OLAPImplementor();
        kapImplementor.visitChild(queryRoot.getInput(0), queryRoot);
        QueryContext.current().record("collect_olap_context_info");
        // identify model
        List<OLAPContext> contexts = ContextUtil.listContextsHavingScan();

        for (OLAPContext olapContext : contexts) {
            olapContext.setHasSelected(true);
            log.info("Context for realization matching: {}", olapContext);
        }

        long selectLayoutStartTime = System.currentTimeMillis();
        if (contexts.size() > 1) {
            RealizationChooser.multiThreadSelectLayoutCandidate(contexts);
        } else {
            RealizationChooser.selectLayoutCandidate(contexts);
        }
        log.info("select layout candidate for {} olapContext cost {} ms", contexts.size(),
                System.currentTimeMillis() - selectLayoutStartTime);
        QueryContext.current().record("end select realization");
        return contexts;
    }

    // ============================================================================

    static void cutContext(ICutContextStrategy strategy, OLAPRel rootOfSubCtxTree, RelNode queryRoot) {
        if (strategy.needCutOff(rootOfSubCtxTree)) {
            strategy.cutOffContext(rootOfSubCtxTree, queryRoot);
        }
        if (strategy instanceof ContextInitialCutStrategy) {
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
                    throw new NoStreamingRealizationFoundException(STREAMING_TABLE_NOT_SUPPORT_AUTO_MODELING,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getStreamingTableNotSupportAutoModeling()));
                }
            }
        }
        throw new NoRealizationFoundException("No realization found for auto modeling.");
    }
}
