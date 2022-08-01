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

package org.apache.kylin.query.pushdown;

import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.ClassLoaderUtils;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class SparkSubmitter {
    public static final Logger logger = LoggerFactory.getLogger(SparkSubmitter.class);
    private OverriddenSparkSession overriddenSparkSession;

    public static SparkSubmitter getInstance() {
        return Singletons.getInstance(SparkSubmitter.class);
    }

    public OverriddenSparkSession overrideSparkSession(SparkSession ss) {
        this.overriddenSparkSession = new OverriddenSparkSession(ss);
        return overriddenSparkSession;
    }

    public void clearOverride() {
        this.overriddenSparkSession = null;
    }

    private SparkSession getSparkSession() {
        return overriddenSparkSession != null ? overriddenSparkSession.ss : SparderEnv.getSparkSession();
    }

    public PushdownResponse submitPushDownTask(String sql, String project) {
        if (UnitOfWork.isAlreadyInTransaction()) {
            logger.warn("execute spark job with transaction lock");
        }
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        SparkSession ss = getSparkSession();
        val results = SparkSqlClient.executeSqlToIterable(ss, sql, RandomUtil.randomUUID(), project);
        return new PushdownResponse(results._3(), results._1(), (int) results._2());
    }

    public class OverriddenSparkSession implements AutoCloseable {

        private SparkSession ss;

        public OverriddenSparkSession(SparkSession ss) {
            this.ss = ss;
        }

        @Override
        public void close() throws Exception {
            SparkSubmitter.this.clearOverride();
        }
    }
}
