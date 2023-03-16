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

package org.apache.kylin.engine.spark.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cluster.IClusterManager;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.spark.SparkConf;
import org.apache.spark.conf.rule.ExecutorCoreRule;
import org.apache.spark.conf.rule.ExecutorInstancesRule;
import org.apache.spark.conf.rule.ExecutorMemoryRule;
import org.apache.spark.conf.rule.ExecutorOverheadRule;
import org.apache.spark.conf.rule.ShufflePartitionsRule;
import org.apache.spark.conf.rule.SparkConfRule;
import org.apache.spark.conf.rule.StandaloneConfRule;
import org.apache.spark.conf.rule.YarnConfRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class SparkConfHelper {
    protected static final Logger logger = LoggerFactory.getLogger(SparkConfHelper.class);

    // options for helping set spark conf
    private HashMap<String, String> options = Maps.newHashMap();
    // spark configurations
    private HashMap<String, String> confs = Maps.newHashMap();

    private IClusterManager clusterManager;

    // options key
    public static final String SOURCE_TABLE_SIZE = "source_table_size";
    public static final String LAYOUT_SIZE = "layout_size";
    // configurations key
    public static final String DEFAULT_QUEUE = "spark.yarn.queue";
    public static final String REQUIRED_CORES = "required_cores";
    public static final String EXECUTOR_INSTANCES = "spark.executor.instances";
    public static final String EXECUTOR_CORES = "spark.executor.cores";
    public static final String EXECUTOR_MEMORY = "spark.executor.memory";
    public static final String EXECUTOR_OVERHEAD = "spark.executor.memoryOverhead";
    public static final String SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";
    public static final String DRIVER_MEMORY = "spark.driver.memory";
    public static final String DRIVER_OVERHEAD = "spark.driver.memoryOverhead";
    public static final String DRIVER_CORES = "spark.driver.cores";
    public static final String MAX_CORES = "spark.cores.max";
    public static final String COUNT_DISTICT = "count_distinct";

    private static final List<SparkConfRule> EXECUTOR_RULES = Lists.newArrayList(new ExecutorMemoryRule(),
            new ExecutorCoreRule(), new ExecutorOverheadRule(), new ExecutorInstancesRule(),
            new ShufflePartitionsRule(), new StandaloneConfRule(), new YarnConfRule());

    public void generateSparkConf() {
        KylinConfig.getInstanceFromEnv().getSparkBuildConfExtraRules()
                .forEach(rule -> EXECUTOR_RULES.add((SparkConfRule) ClassUtil.newInstance(rule)));
        EXECUTOR_RULES.forEach(sparkConfRule -> sparkConfRule.apply(this));
    }

    public String getOption(String key) {
        return options.getOrDefault(key, null);
    }

    public void setOption(String key, String value) {
        options.put(key, value);
    }

    public void setConf(String key, String value) {
        confs.put(key, value);
    }

    public String getConf(String key) {
        return confs.getOrDefault(key, null);
    }

    public IClusterManager getClusterManager() {
        return clusterManager;
    }

    public void setClusterManager(IClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void applySparkConf(SparkConf sparkConf) throws JsonProcessingException {
        KylinBuildEnv.get().buildJobInfos().recordAutoSparkConfs(confs);
        logger.info("Auto set spark conf: {}", JsonUtil.writeValueAsString(confs));
        for (Map.Entry<String, String> entry : confs.entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }
    }

    public boolean hasCountDistinct() {
        return "true".equalsIgnoreCase(getConf(COUNT_DISTICT));
    }
}
