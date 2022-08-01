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
package org.apache.kylin.query.engine;

import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_DIST_META_URL;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_JOB_ID;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_QUERY_CONTEXT;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_QUERY_PARAMS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;

public class AsyncQueryJobTest extends NLocalFileMetadataTestCase {

    final static String BUILD_HADOOP_CONF = "kylin.engine.submit-hadoop-conf-dir";
    final static String BUILD_HADOOP_CONF_VALUE = "/home/kylin/hadoop_conf";

    final static String ASYNC_HADOOP_CONF = "kylin.query.async-query.submit-hadoop-conf-dir";
    final static String ASYNC_HADOOP_CONF_VALUE = "/home/kylin/hadoop_conf_async";
    final static String ASYNC_QUERY_CLASS = "-className org.apache.kylin.query.engine.AsyncQueryApplication";
    final static String ASYNC_QUERY_SPARK_EXECUTOR_CORES = "kylin.query.async-query.spark-conf.spark.executor.cores";
    final static String ASYNC_QUERY_SPARK_EXECUTOR_MEMORY = "kylin.query.async-query.spark-conf.spark.executor.memory";
    final static String ASYNC_QUERY_SPARK_QUEUE = "root.quard";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testAsyncQueryJob() throws ExecuteException, JsonProcessingException, ShellException {
        CliCommandExecutor executor = Mockito.spy(new CliCommandExecutor());
        Mockito.doReturn(new CliCommandExecutor.CliCmdExecResult(0, "mock", "mock")).when(executor)
                .execute(Mockito.any(), Mockito.any(), Mockito.any());
        AsyncQueryJob asyncQueryJob = Mockito.spy(new AsyncQueryJob());
        Assert.assertNotNull(asyncQueryJob.getCliCommandExecutor());
        Mockito.doNothing().when(asyncQueryJob).killOrphanApplicationIfExists(Mockito.any());
        Mockito.doReturn(executor).when(asyncQueryJob).getCliCommandExecutor();

        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);
        asyncQueryJob.setProject(queryParams.getProject());
        Assert.assertTrue(asyncQueryJob.submit(queryParams).succeed());

        Mockito.doReturn(null).when(asyncQueryJob).getCliCommandExecutor();
        Assert.assertFalse(asyncQueryJob.submit(queryParams).succeed());
    }

    @Test
    public void testAsyncQueryJob_SetHadoopConf() throws ExecuteException, JsonProcessingException {
        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);
        overwriteSystemProp(BUILD_HADOOP_CONF, BUILD_HADOOP_CONF_VALUE);
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                @Override
                protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                    Assert.assertEquals(BUILD_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));

                    val desc = this.getSparkAppDesc();
                    desc.setHadoopConfDir(hadoopConf);
                    desc.setKylinJobJar(kylinJobJar);
                    desc.setAppArgs(appArgs);
                    String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                    return ExecuteResult.createSucceed(cmd);
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            Assert.assertTrue(asyncQueryJob.submit(queryParams).succeed());
        }

        overwriteSystemProp(ASYNC_HADOOP_CONF, ASYNC_HADOOP_CONF_VALUE);
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                @Override
                protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                    Assert.assertEquals(ASYNC_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                    val desc = this.getSparkAppDesc();
                    desc.setHadoopConfDir(hadoopConf);
                    desc.setKylinJobJar(kylinJobJar);
                    desc.setAppArgs(appArgs);
                    String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                    return ExecuteResult.createSucceed(cmd);
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            Assert.assertTrue(asyncQueryJob.submit(queryParams).succeed());
        }
    }

    @Test
    public void testDumpMetadataAndCreateArgsFile() throws ExecuteException, IOException {
        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);
        queryParams.setSparkQueue(ASYNC_QUERY_SPARK_QUEUE);
        queryParams.setAclInfo(new QueryContext.AclInfo("user1", Sets.newHashSet("group1"), false));
        QueryContext queryContext = QueryContext.current();
        queryContext.setUserSQL("select 1");
        queryContext.getMetrics().setServer("localhost");
        queryContext.getQueryTagInfo().setAsyncQuery(true);

        overwriteSystemProp(ASYNC_HADOOP_CONF, ASYNC_HADOOP_CONF_VALUE);
        AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
            @Override
            protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                Assert.assertEquals(ASYNC_HADOOP_CONF_VALUE, hadoopConf);
                Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                val desc = this.getSparkAppDesc();
                desc.setHadoopConfDir(hadoopConf);
                desc.setKylinJobJar(kylinJobJar);
                desc.setAppArgs(appArgs);
                String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                return ExecuteResult.createSucceed(appArgs
                        .substring(appArgs.lastIndexOf("file:") + "file:".length(), appArgs.lastIndexOf("/")).trim());
            }
        };
        asyncQueryJob.setProject(queryParams.getProject());
        ExecuteResult executeResult = asyncQueryJob.submit(queryParams);
        Assert.assertTrue(executeResult.succeed());
        String asyncQueryJobPath = executeResult.output();
        FileSystem workingFileSystem = HadoopUtil.getWorkingFileSystem();
        Assert.assertTrue(workingFileSystem.exists(new Path(asyncQueryJobPath)));
        Assert.assertEquals(2, workingFileSystem.listStatus(new Path(asyncQueryJobPath)).length);

        FileStatus[] jobFileStatuses = workingFileSystem.listStatus(new Path(asyncQueryJobPath));
        Comparator<FileStatus> fileStatusComparator = new Comparator<FileStatus>() {
            @Override
            public int compare(FileStatus o1, FileStatus o2) {
                return o1.getPath().toString().compareTo(o2.getPath().toString());
            }
        };
        Arrays.sort(jobFileStatuses, fileStatusComparator);

        // validate spark job args
        testSparkArgs(asyncQueryJobPath, workingFileSystem.open(jobFileStatuses[1].getPath()), jobFileStatuses[1]);

        FileStatus[] metaFileStatus = workingFileSystem.listStatus(jobFileStatuses[0].getPath());
        Arrays.sort(metaFileStatus, fileStatusComparator);

        // validate kylin properties
        testKylinConfig(workingFileSystem, metaFileStatus[0]);

        // validate metadata
        testMetadata(workingFileSystem, metaFileStatus[1]);
    }

    private void testMetadata(FileSystem workingFileSystem, FileStatus metaFileStatus) throws IOException {
        val rawResourceMap = Maps.<String, RawResource> newHashMap();
        FileStatus metadataFile = metaFileStatus;
        try (FSDataInputStream inputStream = workingFileSystem.open(metadataFile.getPath());
                ZipInputStream zipIn = new ZipInputStream(inputStream)) {
            ZipEntry zipEntry = null;
            while ((zipEntry = zipIn.getNextEntry()) != null) {
                if (!zipEntry.getName().startsWith("/")) {
                    continue;
                }
                long t = zipEntry.getTime();
                RawResource raw = new RawResource(zipEntry.getName(), ByteSource.wrap(IOUtils.toByteArray(zipIn)), t,
                        0);
                rawResourceMap.put(zipEntry.getName(), raw);
            }
        }
        Assert.assertEquals(81, rawResourceMap.size());
    }

    private void testKylinConfig(FileSystem workingFileSystem, FileStatus metaFileStatus) throws IOException {
        FileStatus kylinPropertiesFile = metaFileStatus;
        Properties properties = new Properties();
        try (FSDataInputStream inputStream = workingFileSystem.open(kylinPropertiesFile.getPath())) {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
            properties.load(br);
        }
        Assert.assertTrue(properties.size() > 0);
        Assert.assertFalse(properties.getProperty("kylin.query.queryhistory.url").contains("hdfs"));
        Assert.assertEquals(ASYNC_QUERY_SPARK_QUEUE,
                properties.getProperty("kylin.query.async-query.spark-conf.spark.yarn.queue"));
        Assert.assertEquals("5", properties.getProperty(ASYNC_QUERY_SPARK_EXECUTOR_CORES));
        Assert.assertEquals("12288m", properties.getProperty(ASYNC_QUERY_SPARK_EXECUTOR_MEMORY));
    }

    private void testSparkArgs(String asyncQueryJobPath, FSDataInputStream open, FileStatus jobFileStatus)
            throws IOException {
        String argsLine = null;
        try (FSDataInputStream inputStream = open) {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
            argsLine = br.readLine();
        }
        Assert.assertNotNull(argsLine);
        Map<String, String> argsMap = JsonUtil.readValueAsMap(argsLine);
        Assert.assertTrue(argsMap.get(P_JOB_ID).startsWith(ASYNC_QUERY_JOB_ID_PRE));
        QueryParams readQueryParams = JsonUtil.readValue(argsMap.get(P_QUERY_PARAMS), QueryParams.class);
        Assert.assertEquals("select 1", readQueryParams.getSql());
        Assert.assertEquals(ASYNC_QUERY_SPARK_QUEUE, readQueryParams.getSparkQueue());
        Assert.assertEquals("default", readQueryParams.getProject());
        Assert.assertEquals("user1", readQueryParams.getAclInfo().getUsername());
        Assert.assertEquals("[group1]", readQueryParams.getAclInfo().getGroups().toString());
        QueryContext readQueryContext = JsonUtil.readValue(argsMap.get(P_QUERY_CONTEXT), QueryContext.class);
        Assert.assertEquals("select 1", readQueryContext.getUserSQL());
        Assert.assertEquals("localhost", readQueryContext.getMetrics().getServer());
        Assert.assertTrue(readQueryContext.getQueryTagInfo().isAsyncQuery());
        Assert.assertTrue(argsMap.get(P_DIST_META_URL).contains(
                asyncQueryJobPath.substring(asyncQueryJobPath.indexOf("working-dir/") + "working-dir/".length())));
    }

    @Test
    public void testJobNotModifyKylinConfig() throws ExecuteException, IOException {
        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);

        overwriteSystemProp(ASYNC_HADOOP_CONF, ASYNC_HADOOP_CONF_VALUE);
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                @Override
                protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                    Assert.assertEquals(ASYNC_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                    val desc = this.getSparkAppDesc();
                    desc.setHadoopConfDir(hadoopConf);
                    desc.setKylinJobJar(kylinJobJar);
                    desc.setAppArgs(appArgs);
                    String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                    return ExecuteResult.createSucceed(
                            appArgs.substring(appArgs.lastIndexOf("file:") + "file:".length(), appArgs.lastIndexOf("/"))
                                    .trim());
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            ExecuteResult executeResult = asyncQueryJob.submit(queryParams);
            Assert.assertTrue(executeResult.succeed());
        }
        Assert.assertFalse(KylinConfig.getInstanceFromEnv().getMetadataUrl().toString().contains("hdfs"));
    }

    @Test
    public void testJobSparkCmd() throws ExecuteException, IOException {
        QueryParams queryParams = new QueryParams("default", "select 1", "", false, true, true);
        queryParams.setSparkQueue(ASYNC_QUERY_SPARK_QUEUE);

        overwriteSystemProp(ASYNC_HADOOP_CONF, ASYNC_HADOOP_CONF_VALUE);
        overwriteSystemProp(ASYNC_QUERY_SPARK_EXECUTOR_CORES, "3");
        overwriteSystemProp(ASYNC_QUERY_SPARK_EXECUTOR_MEMORY, "513m");
        {
            AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
                @Override
                protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                    Assert.assertEquals(ASYNC_HADOOP_CONF_VALUE, hadoopConf);
                    Assert.assertTrue(appArgs.contains(ASYNC_QUERY_CLASS));
                    val desc = this.getSparkAppDesc();
                    desc.setHadoopConfDir(hadoopConf);
                    desc.setKylinJobJar(kylinJobJar);
                    desc.setAppArgs(appArgs);
                    String cmd = (String) this.sparkJobHandler.generateSparkCmd(getConfig(), desc);
                    return ExecuteResult.createSucceed(cmd);
                }
            };
            asyncQueryJob.setProject(queryParams.getProject());
            ExecuteResult executeResult = asyncQueryJob.submit(queryParams);
            Assert.assertTrue(executeResult.succeed());
            Assert.assertTrue(executeResult.output().contains("--conf 'spark.executor.memory=513m'"));
            Assert.assertTrue(executeResult.output().contains("--conf 'spark.executor.cores=3'"));
        }
    }
}
