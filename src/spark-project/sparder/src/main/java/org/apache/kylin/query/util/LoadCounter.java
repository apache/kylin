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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.status.api.v1.ExecutorSummary;
import org.apache.spark.status.api.v1.StageData;
import org.apache.spark.status.api.v1.StageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;
import scala.collection.JavaConverters;

public class LoadCounter {

    private static final long PERIOD_SECONDS = KylinConfig.getInstanceFromEnv().getLoadCounterPeriodSeconds();
    private static final Logger logger = LoggerFactory.getLogger(LoadCounter.class);

    private CircularFifoQueue<Integer> pendingQueue = new CircularFifoQueue(
            KylinConfig.getInstanceFromEnv().getLoadCounterCapacity());

    public static LoadCounter getInstance() {
        return Singletons.getInstance(LoadCounter.class, v -> new LoadCounter());
    }

    private static double median(List<Integer> total) {
        double j;
        Collections.sort(total);
        int size = total.size();
        if (size == 0) {
            return 0;
        }
        if (size % 2 == 1) {
            j = total.get((size - 1) / 2);
        } else {
            j = (total.get(size / 2 - 1) + total.get(size / 2) + 0.0) / 2;
        }
        return j;
    }

    public void init(ScheduledExecutorService executorService) {
        logger.info("Start load pending task");
        executorService.scheduleWithFixedDelay(this::fetchTaskCount, 20, PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    void fetchTaskCount() {
        try {
            pendingQueue.add(getPendingTaskCount());
        } catch (Exception ex) {
            logger.error("Error when fetch spark pending task", ex);
        }
    }

    public int getPendingTaskCount() {
        val activeStage = SparderEnv.getSparkSession().sparkContext().statusStore().activeStages();
        return JavaConverters.seqAsJavaList(activeStage).stream().filter(stage -> StageStatus.ACTIVE == stage.status())
                .map(stageData -> stageData.numTasks() - stageData.numActiveTasks() - stageData.numCompleteTasks())
                .mapToInt(i -> i).sum();
    }

    public int getRunningTaskCount() {
        val activeStage = SparderEnv.getSparkSession().sparkContext().statusStore().activeStages();
        return JavaConverters.seqAsJavaList(activeStage).stream().filter(stage -> StageStatus.ACTIVE == stage.status())
                .map(StageData::numActiveTasks).mapToInt(i -> i).sum();

    }

    public LoadDesc getLoadDesc() {
        val points = new ArrayList<>(pendingQueue);
        logger.trace("Points is {}", points);
        val mean = median(points);
        val executorSummary = SparderEnv.getSparkSession().sparkContext().statusStore().executorList(true);
        val coreNum = JavaConverters.seqAsJavaList(executorSummary).stream().map(ExecutorSummary::totalCores)
                .mapToInt(i -> i).sum();
        val loadDesc = new LoadDesc(mean / coreNum, coreNum, points);
        logger.debug("LoadDesc is {}", loadDesc);
        return loadDesc;
    }

    public int getSlotCount() {
        return SparderEnv.getTotalCore();
    }

}
