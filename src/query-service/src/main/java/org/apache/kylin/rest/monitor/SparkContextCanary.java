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
package org.apache.kylin.rest.monitor;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.val;

public class SparkContextCanary {
    private static final Logger logger = LoggerFactory.getLogger(SparkContextCanary.class);

    private static final int THRESHOLD_TO_RESTART_SPARK = KapConfig.getInstanceFromEnv().getThresholdToRestartSpark();
    private static final int PERIOD_MINUTES = KapConfig.getInstanceFromEnv().getSparkCanaryPeriodMinutes();
    private static final String WORKING_DIR = KapConfig.getInstanceFromEnv().getWriteHdfsWorkingDirectory();
    private static final String CHECK_TYPE = KapConfig.getInstanceFromEnv().getSparkCanaryType();

    @Getter
    private volatile int errorAccumulated = 0;
    @Getter
    private volatile long lastResponseTime = -1;
    @Getter
    private volatile boolean sparkRestarting = false;

    private SparkContextCanary() {
    }

    public static SparkContextCanary getInstance() {
        return Singletons.getInstance(SparkContextCanary.class, v -> new SparkContextCanary());
    }

    public void init(ScheduledExecutorService executorService) {
        logger.info("Start monitoring Spark");
        executorService.scheduleWithFixedDelay(this::monitor, PERIOD_MINUTES, PERIOD_MINUTES, TimeUnit.MINUTES);
    }

    public boolean isError() {
        return errorAccumulated >= THRESHOLD_TO_RESTART_SPARK;
    }

    void monitor() {
        try {
            long startTime = System.currentTimeMillis();
            // check spark sql context
            if (!SparderEnv.isSparkAvailable()) {
                logger.info("Spark is unavailable, need to restart immediately.");
                errorAccumulated = Math.max(errorAccumulated + 1, THRESHOLD_TO_RESTART_SPARK);
            } else {
                Future<Boolean> handler = check();
                try {
                    long t = System.currentTimeMillis();
                    handler.get(KapConfig.getInstanceFromEnv().getSparkCanaryErrorResponseMs(), TimeUnit.MILLISECONDS);
                    logger.info("SparkContextCanary checkWriteFile returned successfully, takes {} ms.",
                            (System.currentTimeMillis() - t));
                    // reset errorAccumulated once good context is confirmed
                    errorAccumulated = 0;
                } catch (TimeoutException te) {
                    errorAccumulated++;
                    handler.cancel(true);
                    logger.error("SparkContextCanary write file timeout.", te);
                } catch (InterruptedException e) {
                    errorAccumulated++;
                    logger.error("Thread is interrupted.", e);
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    errorAccumulated = Math.max(errorAccumulated + 1, THRESHOLD_TO_RESTART_SPARK);
                    logger.error("SparkContextCanary numberCount occurs exception, need to restart immediately.", e);
                } catch (Exception e) {
                    errorAccumulated++;
                    logger.error("SparkContextCanary write file occurs exception.", e);
                }
            }

            lastResponseTime = System.currentTimeMillis() - startTime;
            logger.debug("Spark context errorAccumulated:{}", errorAccumulated);

            if (isError()) {
                sparkRestarting = true;
                try {
                    // Take repair action if error accumulated exceeds threshold
                    logger.warn("Repairing spark context");
                    SparderEnv.restartSpark();

                    MetricsGroup.hostTagCounterInc(MetricsName.SPARDER_RESTART, MetricsCategory.GLOBAL, "global");
                } catch (Throwable th) {
                    logger.error("Restart spark context failed.", th);
                }
                sparkRestarting = false;
            }
        } catch (Throwable th) {
            logger.error("Error when monitoring Spark.", th);
            Thread.currentThread().interrupt();
        }
    }

    // for canary
    private Future<Boolean> check() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        return executor.submit(() -> {
            val ss = SparderEnv.getSparkSession();
            val jsc = JavaSparkContext.fromSparkContext(SparderEnv.getSparkSession().sparkContext());
            jsc.setLocalProperty("spark.scheduler.pool", "vip_tasks");
            jsc.setJobDescription("Canary check by " + CHECK_TYPE);

            switch (CHECK_TYPE) {
            case "file":
                val rowList = new ArrayList<Row>();
                for (int i = 0; i < 100; i++) {
                    rowList.add(RowFactory.create(i));
                }

                val schema = new StructType(
                        new StructField[] { DataTypes.createStructField("col", DataTypes.IntegerType, true) });
                val df = ss.createDataFrame(jsc.parallelize(rowList), schema);
                val appId = ss.sparkContext().applicationId();
                df.write().mode(SaveMode.Overwrite).parquet(WORKING_DIR + "/_health/" + appId);
                break;
            case "count":
                val countList = new ArrayList<Integer>();
                for (int i = 0; i < 100; i++) {
                    countList.add(i);
                }
                jsc.parallelize(countList).count();
                break;
            default:
                break;
            }
            jsc.setJobDescription(null);
            return true;
        });
    }
}
