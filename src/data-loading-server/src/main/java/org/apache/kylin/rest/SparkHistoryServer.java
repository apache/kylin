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

package org.apache.kylin.rest;

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.history.HistoryServer;
import org.apache.spark.deploy.history.HistoryServerBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.SneakyThrows;

@Configuration
@ConditionalOnProperty(name = "kylin.history-server.enable", havingValue = "true")
public class SparkHistoryServer {

    @Bean("historyServer")
    @SneakyThrows
    public HistoryServer createHistoryServer() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        SparkConf sparkConf = new SparkConf();
        Map<String, String> sparkConfigOverride = config.getSparkConfigOverride();
        for (Map.Entry<String, String> entry : sparkConfigOverride.entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }

        String logDir = sparkConf.get("spark.eventLog.dir");

        Path logPath = new Path(new URI(logDir).getPath());
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (!fs.exists(logPath)) {
            fs.mkdirs(logPath);
        }
        sparkConf.set("spark.history.fs.logDirectory", sparkConf.get("spark.eventLog.dir"));
        return HistoryServerBuilder.createHistoryServer(sparkConf);
    }
}
