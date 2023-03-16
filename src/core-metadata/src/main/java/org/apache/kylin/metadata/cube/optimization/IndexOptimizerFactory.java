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

package org.apache.kylin.metadata.cube.optimization;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.metadata.cube.model.NDataflow;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class IndexOptimizerFactory {

    private IndexOptimizerFactory() {
    }

    private static final AbstractOptStrategy INCLUDED_OPT_STRATEGY = new IncludedLayoutOptStrategy();
    private static final AbstractOptStrategy LOW_FREQ_OPT_STRATEGY = new LowFreqLayoutOptStrategy();
    private static final AbstractOptStrategy SIMILAR_OPT_STRATEGY = new SimilarLayoutOptStrategy();

    public static IndexOptimizer getOptimizer(NDataflow dataflow, boolean needLog) {
        IndexOptimizer optimizer = new IndexOptimizer(needLog);
        final int indexOptimizationLevel = KylinConfig.getInstanceFromEnv().getIndexOptimizationLevel();
        if (indexOptimizationLevel == 1) {
            optimizer.getStrategiesForAuto().add(INCLUDED_OPT_STRATEGY);
        } else if (indexOptimizationLevel == 2) {
            optimizer.getStrategiesForAuto().addAll(Lists.newArrayList(INCLUDED_OPT_STRATEGY, LOW_FREQ_OPT_STRATEGY));
        } else if (indexOptimizationLevel == 3) {
            optimizer.getStrategiesForAuto().addAll(Lists.newArrayList(INCLUDED_OPT_STRATEGY, LOW_FREQ_OPT_STRATEGY));
            optimizer.getStrategiesForManual().add(SIMILAR_OPT_STRATEGY);
        }

        // log if needed
        printLog(needLog, indexOptimizationLevel, dataflow.getIndexPlan().isFastBitmapEnabled());
        return optimizer;
    }

    private static void printLog(boolean needLog, int indexOptimizationLevel, boolean isFastBitmapEnabled) {
        if (!needLog) {
            return;
        }

        if (indexOptimizationLevel == 3 && isFastBitmapEnabled) {
            log.info("Routing to index optimization level two for fastBitMap enabled.");
        } else if (indexOptimizationLevel == 3 || indexOptimizationLevel == 2 || indexOptimizationLevel == 1) {
            log.info("Routing to index optimization level " + indexOptimizationLevel + ".");
        } else if (indexOptimizationLevel == 0) {
            log.info("Routing to index optimization level zero, no optimization.");
        } else {
            log.error("Not supported index optimization level");
        }
    }
}
