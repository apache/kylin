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
package org.apache.kylin.rest.service;

import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_DIST_META_URL;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_JOB_ID;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_QUERY_CONTEXT;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_QUERY_PARAMS;
import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.extension.KylinInfoExtension;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class, KylinInfoExtension.class })
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
public class AsyncQueryJobTest extends NLocalFileMetadataTestCase {

    static final String BUILD_HADOOP_CONF = "kylin.engine.submit-hadoop-conf-dir";
    static final String BUILD_HADOOP_CONF_VALUE = "/home/kylin/hadoop_conf";

    static final String ASYNC_HADOOP_CONF = "kylin.query.async-query.submit-hadoop-conf-dir";
    static final String ASYNC_HADOOP_CONF_VALUE = "/home/kylin/hadoop_conf_async";
    static final String ASYNC_QUERY_CLASS = "-className org.apache.kylin.query.engine.AsyncQueryApplication";
    static final String ASYNC_QUERY_SPARK_EXECUTOR_CORES = "kylin.query.async-query.spark-conf.spark.executor.cores";
    static final String ASYNC_QUERY_SPARK_EXECUTOR_MEMORY = "kylin.query.async-query.spark-conf.spark.executor.memory";
    static final String ASYNC_QUERY_SPARK_QUEUE = "root.quard";

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        // Use thenAnswer instead of thenReturn, a workaround for https://github.com/powermock/powermock/issues/992
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenAnswer((invocation -> userGroupInformation));
        PowerMockito.when(UserGroupInformation.getLoginUser()).thenAnswer((invocation -> userGroupInformation));
        PowerMockito.mockStatic(SpringContext.class);
        JobInfoMapper jobInfoMapper = Mockito.spy(JobInfoMapper.class);
        Mockito.when(jobInfoMapper.selectByJobFilter(Mockito.any(JobMapperFilter.class))).thenReturn(new ArrayList<>());
        JobInfoDao jobInfoDao = Mockito.spy(JobInfoDao.class);
        ReflectionTestUtils.setField(jobInfoDao, "jobInfoMapper", jobInfoMapper);
        PowerMockito.when(SpringContext.getBean(JobInfoDao.class)).thenAnswer(invocation -> jobInfoDao);

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
        val rawResourceMap = Maps.<String, RawResource> newTreeMap();
        FileStatus metadataFile = metaFileStatus;
        try (FSDataInputStream inputStream = workingFileSystem.open(metadataFile.getPath());
                ZipInputStream zipIn = new ZipInputStream(inputStream)) {
            ZipEntry zipEntry = null;
            while ((zipEntry = zipIn.getNextEntry()) != null) {
                long t = zipEntry.getTime();
                RawResource raw = new RawResource(zipEntry.getName(), ByteSource.wrap(IOUtils.toByteArray(zipIn)), t,
                        0);
                rawResourceMap.put(zipEntry.getName(), raw);
            }
        }
        Assert.assertEquals(118, rawResourceMap.size());
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

    @Test
    public void testModifyDump() {
        AsyncQueryJob asyncQueryJob = new AsyncQueryJob() {
            @Override
            protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
                return ExecuteResult.createSucceed();
            }
        };

        val properties = new Properties();
        properties.setProperty("kylin.extension.info.factory",
                "org.apache.kylin.common.extension.KylinInfoExtension$Factory");
        properties.setProperty("kylin.streaming.enabled", "true");

        val kylinInfoExtensionFactory = Mockito.mock(KylinInfoExtension.Factory.class);
        PowerMockito.mockStatic(KylinInfoExtension.class);
        PowerMockito.when(KylinInfoExtension.getFactory()).thenReturn(kylinInfoExtensionFactory);

        Mockito.when(kylinInfoExtensionFactory.checkKylinInfo()).thenReturn(false);
        val properties1 = new Properties();
        properties1.putAll(properties);
        asyncQueryJob.modifyDump(properties1);
        Assert.assertNull(properties1.get("kylin.extension.info.factory"));
        Assert.assertEquals("false", properties1.get("kylin.streaming.enabled"));

        Mockito.when(kylinInfoExtensionFactory.checkKylinInfo()).thenReturn(true);
        val properties2 = new Properties();
        properties2.putAll(properties);
        asyncQueryJob.modifyDump(properties2);
        Assert.assertNull(properties2.get("kylin.extension.info.factory"));
        Assert.assertEquals(properties.getProperty("kylin.streaming.enabled"),
                properties2.get("kylin.streaming.enabled"));
    }
}
