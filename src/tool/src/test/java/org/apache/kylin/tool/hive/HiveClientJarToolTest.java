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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import lombok.val;

//@MetadataInfo
class HiveClientJarToolTest extends HiveClientJarToolTestBase {

    @BeforeEach
    public void before() throws IOException {
        super.before();
        val sparkHome = KylinConfig.getSparkHome();
        val sparkPath = Paths.get(sparkHome);
        val hive122 = sparkHome + File.separator + "hive_1_2_2";
        val hive122Path = Paths.get(hive122);
        val jar = hive122 + File.separator + "test.jar";
        val jarPath = Paths.get(jar);
        Files.createDirectories(hive122Path);
        Files.createFile(jarPath);
    }

    @AfterEach
    public void after() throws IOException {
        super.after();
        val sparkHome = KylinConfig.getSparkHome();
        val sparkPath = Paths.get(sparkHome);
        val hive122 = sparkHome + File.separator + "hive_1_2_2";
        val hive122Path = Paths.get(hive122);
        val jar = hive122 + File.separator + "test.jar";
        val jarPath = Paths.get(jar);
        Files.deleteIfExists(jarPath);
        Files.deleteIfExists(hive122Path);
        Files.deleteIfExists(sparkPath);
    }

    //    @Test
    void uploadHiveJars() throws IOException {
        uploadHiveJars(false);
        uploadHiveJars(true);
    }

    void uploadHiveJars(boolean exist) throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val hive = new Path(config.getHdfsWorkingDirectory(), "hive");
        val uploadHiveJarFlag = new Path(config.getHdfsWorkingDirectory(), "upload_hive_jar");
        try {
            config.setProperty("kylin.engine.spark-conf.spark.sql.hive.metastore.jars.path",
                    config.getHdfsWorkingDirectory() + "hive/*");

            if (exist) {
                fileSystem.mkdirs(hive);
            }
            val kylinSparkHiveJarsPath = uploadHiveJarsTool.getKylinSparkHiveJarsPath();
            val sparkSqlHiveMetastoreJarsPath = config.getSparkSqlHiveMetastoreJarsPath();
            val jarsDirPath = new Path(sparkSqlHiveMetastoreJarsPath).getParent();
            uploadHiveJarsTool.uploadHiveJars(fileSystem, uploadHiveJarFlag, kylinSparkHiveJarsPath, jarsDirPath);

            if (exist) {
                ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
                Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
                val logs = logCaptor.getAllValues().stream()
                        .filter(event -> event.getLoggerName().equals("org.apache.kylin.tool.hive.HiveClientJarTool"))
                        .filter(event -> event.getLevel().equals(Level.WARN))
                        .map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
                val logCount = logs.stream()
                        .filter(log -> log.equals("HDFS dir [" + hive + "] exist, not upload hive client jar")).count();
                assertEquals(1, logCount);
            } else {
                val hdfsHiveJarsPath = new Path(config.getSparkSqlHiveMetastoreJarsPath().substring(0,
                        config.getSparkSqlHiveMetastoreJarsPath().length() - 1));
                val fileStatuses = HadoopUtil.getWorkingFileSystem().listStatus(hdfsHiveJarsPath);
                assertEquals(1, fileStatuses.length);
                val hdfsJarPath = fileStatuses[0].getPath();
                assertEquals(hdfsHiveJarsPath + "/test.jar", hdfsJarPath.toString());
            }
            assertEquals(!exist, fileSystem.exists(uploadHiveJarFlag));
        } finally {
            config.setProperty("kylin.engine.spark-conf.spark.sql.hive.metastore.jars.path", "");
            fileSystem.delete(uploadHiveJarFlag, true);
            fileSystem.delete(hive, true);
        }
    }

    //    @Test
    void testExecute() throws IOException {
        testExecute(true, false, "kylin.engine.spark-conf.spark.sql.hive.metastore.jars.path not setting");
        testExecute(true, true, "Upload Spark HIVE jars success");
        testExecute(true, true, "Not need upload Spark HIVE jars again");
        testExecute(false, false, "Not need upload hive client jar");
    }

}
