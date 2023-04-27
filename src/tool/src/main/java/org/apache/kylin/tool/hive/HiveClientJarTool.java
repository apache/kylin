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

package org.apache.kylin.tool.hive;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.FileUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.tool.util.ToolMainWrapper;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HiveClientJarTool {
    public static void main(String[] args) {
        ToolMainWrapper.wrap(args, () -> {
            HiveClientJarTool tool = new HiveClientJarTool();
            tool.execute();
        });
        Unsafe.systemExit(0);
    }

    public void execute() throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        if (!config.getHiveClientJarUploadEnable()) {
            log.info("Not need upload hive client jar");
            return;
        }
        if (StringUtils.isBlank(config.getSparkSqlHiveMetastoreJarsPath())) {
            log.warn("kylin.engine.spark-conf.spark.sql.hive.metastore.jars.path not setting");
            return;
        }

        // In Read/Write Separation Deployment, clusters are cross-domain mutual trust
        // can use ReadClusterFileSystem to access WriteClusterFileSystem
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val sparkSqlHiveMetastoreJarsPath = config.getSparkSqlHiveMetastoreJarsPath();
        val jarsDirPath = new Path(sparkSqlHiveMetastoreJarsPath).getParent();
        val uploadHiveJarFlag = new Path(jarsDirPath, "_upload_hive_jar_by_pass");
        if (fileSystem.exists(uploadHiveJarFlag)) {
            log.info("Not need upload Spark HIVE jars again");
            return;
        }

        val kylinSparkHiveJarsPath = getKylinSparkHiveJarsPath();
        if (StringUtils.isBlank(kylinSparkHiveJarsPath)) {
            log.warn("${KYLIN_HOME}/spark/hive_1_2_2 needs to be an existing directory");
            return;
        }

        uploadHiveJars(fileSystem, uploadHiveJarFlag, kylinSparkHiveJarsPath, jarsDirPath);
    }

    public void uploadHiveJars(FileSystem fileSystem, Path uploadHiveJarFlag, String kylinSparkHiveJarsPath,
            Path jarsDirPath) throws IOException {
        if (fileSystem.exists(jarsDirPath)) {
            log.warn("HDFS dir [{}] exist, not upload hive client jar", jarsDirPath);
            return;
        }
        fileSystem.mkdirs(jarsDirPath);
        val hiveJars = FileUtils.findFiles(kylinSparkHiveJarsPath);
        for (File jar : hiveJars) {
            val sparkHiveJarPath = new Path(jar.getCanonicalPath());
            fileSystem.copyFromLocalFile(sparkHiveJarPath, jarsDirPath);
        }
        log.info("Upload Spark HIVE jars ending");
        try (val out = fileSystem.create(uploadHiveJarFlag, true)) {
            out.write(new byte[] {});
        }
        log.info("Upload Spark HIVE jars success");
    }

    public String getKylinSparkHiveJarsPath() throws IOException {
        String sparkHome = KylinConfig.getSparkHome();
        val jar = FileUtils.findFile(sparkHome, "hive_1_2_2");
        if (jar == null || jar.isFile()) {
            return "";
        }
        return jar.getCanonicalPath();
    }
}
