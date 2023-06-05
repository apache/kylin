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
package org.apache.kylin.tool;

import java.io.File;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.metrics.service.MonitorDao;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class InfluxDBTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final String INFLUXD_PATH = "/usr/bin/influxd";

    private InfluxDBTool() {
    }

    public static void dumpInfluxDBMetrics(File exportDir) {
        KapConfig kapConfig = KapConfig.wrap(KylinConfig.getInstanceFromEnv());
        String database = kapConfig.getMetricsDbNameWithMetadataUrlPrefix();
        dumpInfluxDB(exportDir, "system_metrics", database);
    }

    public static void dumpInfluxDBMonitorMetrics(File exportDir) {
        String database = MonitorDao.generateDatabase(KylinConfig.getInstanceFromEnv());
        dumpInfluxDB(exportDir, "monitor_metrics", database);
    }

    private static void dumpInfluxDB(File exportDir, String subDir, String database) {
        File destDir = new File(exportDir, subDir);

        try {
            KapConfig kapConfig = KapConfig.wrap(KylinConfig.getInstanceFromEnv());
            String host = kapConfig.getMetricsRpcServiceBindAddress();
            FileUtils.forceMkdir(destDir);
            logger.info("Try connect {}.", host);
            String ip = host.split(":")[0];
            int port = Integer.parseInt(host.split(":")[1]);
            if (!ToolUtil.isPortAvailable(ip, port)) {
                logger.info("Failed to Connect influxDB in {}, skip dump.", host);
                return;
            }
            logger.info("Connect successful.");

            File influxd = new File(INFLUXD_PATH);
            if (!influxd.exists()) {
                String influxDBHome = ToolUtil.getKylinHome() + File.separator + "influxdb";
                influxd = new File(influxDBHome + INFLUXD_PATH);
            }

            if (!(StringHelper.validateDbName(database) && StringHelper.validateUrl(host))) {
                throw new IllegalArgumentException(
                        "Something is wrong with Influx database and host: " + database + ", " + host);
            }
            String cmd = String.format(Locale.ROOT, "%s backup -portable -database %s -host %s %s",
                    influxd.getAbsolutePath(), database, host, destDir.getAbsolutePath());
            logger.info("InfluxDB backup cmd is {}.", cmd);

            val result = new CliCommandExecutor().execute(cmd, null);
            if (null != result.getCmd()) {
                logger.debug("dump InfluxDB, database: {}, info: {}", database, result.getCmd());
            }
        } catch (Exception e) {
            logger.debug("Failed to dump influxdb by database: {} ", database, e);
        }
    }
}
