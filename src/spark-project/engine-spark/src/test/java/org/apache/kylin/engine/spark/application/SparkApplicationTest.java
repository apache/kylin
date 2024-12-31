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

package org.apache.kylin.engine.spark.application;

import static org.apache.kylin.job.constant.ExecutableConstants.COLUMNAR_SHUFFLE_MANAGER;
import static org.apache.kylin.job.constant.ExecutableConstants.GLUTEN_PLUGIN;
import static org.apache.kylin.job.constant.ExecutableConstants.SPARK_PLUGINS;
import static org.apache.kylin.job.constant.ExecutableConstants.SPARK_SHUFFLE_MANAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.engine.spark.job.BuildJobInfos;
import org.apache.kylin.engine.spark.job.InternalTableLoadJob;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.engine.spark.job.MockJobProgressReport;
import org.apache.kylin.engine.spark.job.ParamsConstants;
import org.apache.kylin.engine.spark.job.RestfulJobProgressReport;
import org.apache.kylin.engine.spark.job.SegmentBuildJob;
import org.apache.kylin.engine.spark.scheduler.JobFailed;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.application.MockClusterManager;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.val;

public class SparkApplicationTest extends NLocalWithSparkSessionTestBase {

    File tempDir = new File("./temp/");
    File file1 = new File(tempDir, "temp1_" + ResourceDetectUtils.fileName());
    File file2 = new File(tempDir, "temp2_" + ResourceDetectUtils.fileName());

    @Before
    public void before() throws IOException {
        FileUtils.forceMkdir(tempDir);

    }

    @After
    public void after() {
        FileUtils.deleteQuietly(tempDir);
    }

    @Test
    public void testChooseContentSize() throws Exception {
        SparkApplication application = new SparkApplication() {
            @Override
            protected void doExecute() throws Exception {
                System.out.println("empty");
            }
        };

        // write resource_path file
        Map<String, Long> map1 = Maps.newHashMap();
        map1.put("1", 300L);
        ResourceDetectUtils.write(new Path(file1.getAbsolutePath()), map1);

        Map<String, Long> map2 = Maps.newHashMap();
        map2.put("1", 200L);
        ResourceDetectUtils.write(new Path(file2.getAbsolutePath()), map2);

        assertEquals("300b", application.chooseContentSize(new Path(tempDir.getAbsolutePath())));
    }

    @Test
    public void testUpdateSparkJobExtraInfo() throws Exception {
        overwriteSystemProp("spark.driver.param.taskId", "cb91189b-2b12-4527-aa35-0130e7d54ec0_01");

        RestfulJobProgressReport report = Mockito.spy(new RestfulJobProgressReport());

        SparkApplication application = Mockito.spy(new SparkApplication() {
            @Override
            protected void doExecute() throws Exception {
                System.out.println("empty");
            }
        });

        Mockito.doReturn("http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/").when(application)
                .getTrackingUrl(null, ss);

        Map<String, String> payload = new HashMap<>(5);
        payload.put("project", "test_job_output");
        payload.put("job_id", "cb91189b-2b12-4527-aa35-0130e7d54ec0");
        payload.put("task_id", "cb91189b-2b12-4527-aa35-0130e7d54ec0_01");
        payload.put("yarn_app_id", "application_1561370224051_0160");
        payload.put("yarn_app_url", "http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/");

        Map<String, String> extraInfo = new HashMap<>();
        extraInfo.put("yarn_app_id", "application_1561370224051_0160");
        extraInfo.put("yarn_app_url", "http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/");

        String payloadJson = JsonUtil.writeValueAsString(payload);
        Map<String, String> params = new HashMap<>();
        params.put(ParamsConstants.TIME_OUT, String.valueOf(getTestConfig().getUpdateJobInfoTimeout()));
        params.put(ParamsConstants.JOB_TMP_DIR, getTestConfig().getJobTmpDir("test_job_output", true));
        Mockito.doReturn(Boolean.TRUE).when(report).updateSparkJobInfo(params, "/kylin/api/jobs/spark", payloadJson);

        assertTrue(report.updateSparkJobExtraInfo(params, "/kylin/api/jobs/spark", "test_job_output",
                "cb91189b-2b12-4527-aa35-0130e7d54ec0", extraInfo));

        Mockito.verify(report).updateSparkJobInfo(params, "/kylin/api/jobs/spark", payloadJson);

        Mockito.reset(application);
        Mockito.reset(report);
        Mockito.doReturn("http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/").when(application)
                .getTrackingUrl(null, ss);
        Mockito.doReturn(Boolean.FALSE).when(report).updateSparkJobInfo(params, "/kylin/api/jobs/spark", payloadJson);
        assertFalse(report.updateSparkJobExtraInfo(params, "/kylin/api/jobs/spark", "test_job_output",
                "cb91189b-2b12-4527-aa35-0130e7d54ec0", extraInfo));

        Mockito.verify(report, Mockito.times(3)).updateSparkJobInfo(params, "/kylin/api/jobs/spark", payloadJson);
    }

    @Test
    public void testCheckRangePartitionTableIsExist() throws Exception {
        KylinBuildEnv.getOrCreate(getTestConfig());
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "tdh");
        TableDesc fact = tableMgr.getTableDesc("TDH_TEST.LINEORDER_PARTITION");
        fact.setTransactional(true);

        PartitionDesc partitionDesc = new PartitionDesc();
        ColumnDesc columnDesc = new ColumnDesc();
        columnDesc.setName("LO_DATE");
        columnDesc.setDatatype("date");
        columnDesc.setTable(fact);
        NDataModel nDataModel = new NDataModel();
        nDataModel.setUuid(UUID.randomUUID().toString());
        SegmentRange.TimePartitionedSegmentRange timePartitionedSegmentRange //
                = new SegmentRange.TimePartitionedSegmentRange();
        timePartitionedSegmentRange.setStart(1637387522L);
        timePartitionedSegmentRange.setEnd(1637905922L);
        // fact.setSegmentRange(timePartitionedSegmentRange);
        TableRef tableRef = new TableRef(nDataModel, "LINEORDER_PARTITION", fact, false);
        partitionDesc.setPartitionDateColumnRef(new TblColRef(tableRef, columnDesc));
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd hh:mm:ss");
        fact.setPartitionDesc(partitionDesc);

        Set<TableRef> tableRefs = Sets.newHashSet();

        SparkApplication sparkApplication = Mockito.mock(SparkApplication.class);
        Mockito.when(sparkApplication.checkRangePartitionTableIsExist(Mockito.any())).thenCallRealMethod();
        tableRefs.add(tableRef);
        nDataModel.setAllTableRefs(tableRefs);
        assertFalse(sparkApplication.checkRangePartitionTableIsExist(nDataModel));

        NDataModel nDataModel2 = new NDataModel();
        nDataModel2.setUuid(UUID.randomUUID().toString());
        timePartitionedSegmentRange.setStart(1637387522L);
        timePartitionedSegmentRange.setEnd(1637905922L);
        // fact.setSegmentRange(timePartitionedSegmentRange);
        fact.setRangePartition(Boolean.TRUE);
        tableRef = new TableRef(nDataModel2, "LINEORDER_PARTITION", fact, false);
        partitionDesc.setPartitionDateColumnRef(new TblColRef(tableRef, columnDesc));
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd hh:mm:ss");
        fact.setPartitionDesc(partitionDesc);

        tableRefs.clear();
        tableRefs.add(tableRef);
        nDataModel2.setAllTableRefs(tableRefs);
        assertTrue(sparkApplication.checkRangePartitionTableIsExist(nDataModel2));
    }

    @Test
    public void testExtraDestroy() throws IOException {
        KylinConfig config = getTestConfig();
        String path = tempDir.getPath() + "/upload";
        SparkApplication application = new SparkApplication() {
            @Override
            protected void doExecute() {
                // do nothing
            }
        };
        File upload = new File(path);
        FileUtils.forceMkdir(upload);
        assertTrue(upload.exists());
        config.setProperty(config.getKubernetesUploadPathKey(), path);
        ReflectionTestUtils.setField(application, "config", config);
        application.extraDestroy();
        assertFalse(upload.exists());
    }

    @Test
    public void testMkHistoryEventLog() throws Exception {
        KylinConfig config = getTestConfig();
        SparkApplication application = new SparkApplication() {
            @Override
            protected void doExecute() {
                // do nothing
            }
        };
        application.config = config;
        SparkConf sparkConf = new SparkConf();

        Path existedLogDir = new Path("/tmp/ke/testMkHistoryEventLog-existed-" + System.currentTimeMillis());
        Path notExistedLogDir = new Path("/tmp/ke/testMkHistoryEventLog-not-existed-" + System.currentTimeMillis());
        val fs = HadoopUtil.getWorkingFileSystem();
        if (!fs.exists(existedLogDir)) {
            fs.mkdirs(existedLogDir);
        }
        if (fs.exists(notExistedLogDir)) {
            fs.delete(existedLogDir);
        }
        sparkConf.set("spark.eventLog.enabled", "false");
        sparkConf.set("spark.eventLog.dir", notExistedLogDir.toString());
        application.exchangeSparkConf(sparkConf);
        assertFalse(fs.exists(notExistedLogDir));
        sparkConf.set("spark.eventLog.enabled", "true");
        application.exchangeSparkConf(sparkConf);
        assertTrue(fs.exists(notExistedLogDir));
        sparkConf.set("spark.eventLog.dir", existedLogDir.toString());
        application.exchangeSparkConf(sparkConf);
        assertTrue(fs.exists(existedLogDir));
        sparkConf.set("spark.eventLog.dir", "");
        application.exchangeSparkConf(sparkConf);
    }

    @Test
    public void testUpdateJobErrorInfo() throws JsonProcessingException {
        val config = getTestConfig();
        val project = "test_project";
        SparkApplication application = Mockito.spy(new SparkApplication() {
            @Override
            protected void doExecute() {
                // do nothing
            }
        });

        application.config = config;
        application.jobId = "job_id";
        application.project = project;

        BuildJobInfos infos = new BuildJobInfos();
        infos.recordStageId("stage_id");
        infos.recordJobStepId("job_step_id");
        infos.recordSegmentId("segment_id");

        application.infos = infos;
        MockJobProgressReport mockJobProgressReport = Mockito.spy(new MockJobProgressReport());
        Mockito.when(application.getReport()).thenReturn(mockJobProgressReport);

        JobFailed jobFailed = Mockito.mock(JobFailed.class);
        Mockito.when(jobFailed.reason()).thenReturn("test job failed");
        try (MockedStatic<ExceptionUtils> exceptionUtilsMockedStatic = Mockito.mockStatic(ExceptionUtils.class)) {
            exceptionUtilsMockedStatic.when(() -> ExceptionUtils.getStackTrace(jobFailed.throwable()))
                    .thenReturn("test stack trace");
            application.updateJobErrorInfo(jobFailed);
        }

        val paramsMap = new HashMap<String, String>();
        paramsMap.put(ParamsConstants.TIME_OUT, String.valueOf(config.getUpdateJobInfoTimeout()));
        paramsMap.put(ParamsConstants.JOB_TMP_DIR, config.getJobTmpDir(project, true));

        val json = "{\"job_last_running_start_time\":null,\"job_id\":\"job_id\",\"project\":\"test_project\",\"failed_segment_id\":\"segment_id\",\"failed_stack\":\"test stack "
                + "trace\",\"failed_reason\":\"test job failed\",\"failed_step_id\":\"stage_id\"}";

        Mockito.verify(application.getReport(), Mockito.times(1)).updateSparkJobInfo(paramsMap, "/kylin/api/jobs/error",
                json);
    }

    @Test
    public void reportSparkJobExtraInfo() {
        overwriteSystemProp("kylin.env", "PROD");
        overwriteSystemProp("kylin.engine.spark.cluster-manager-class-name",
                MockClusterManager.class.getCanonicalName());
        val appId = RandomUtil.randomUUIDStr();
        val config = getTestConfig();
        val sparkSession = Mockito.mock(SparkSession.class);
        val sparkContext = Mockito.mock(SparkContext.class);
        Mockito.when(sparkSession.sparkContext()).thenReturn(sparkContext);
        Mockito.when(sparkContext.applicationId()).thenReturn(appId);
        Mockito.when(sparkContext.conf()).thenReturn(new SparkConf());
        val application = Mockito.spy(new SparkApplication() {
            @Override
            protected void doExecute() {
                // only for test
            }
        });
        MockJobProgressReport mockJobProgressReport = Mockito.spy(new MockJobProgressReport());
        Mockito.when(application.getReport()).thenReturn(mockJobProgressReport);
        ReflectionTestUtils.setField(application, "config", config);
        val atomicBuildEnv = new AtomicReference<KylinBuildEnv>(KylinBuildEnv.getOrCreate(config));
        ReflectionTestUtils.setField(application, "atomicBuildEnv", atomicBuildEnv);
        application.reportSparkJobExtraInfo(sparkSession);

        val paramsMap = Maps.<String, String> newHashMap();
        paramsMap.put(ParamsConstants.TIME_OUT, String.valueOf(config.getUpdateJobInfoTimeout()));
        paramsMap.put(ParamsConstants.JOB_TMP_DIR, config.getJobTmpDir(null, true));
        val json = Maps.<String, String> newHashMap();
        json.put("queue_name", "default");
        json.put("job_last_running_start_time", null);
        json.put("cores", "0");
        json.put("memory", "0");
        json.put("yarn_app_id", appId);
        Mockito.verify(application.getReport(), Mockito.times(1)).updateSparkJobExtraInfo(paramsMap,
                "/kylin/api/jobs/spark", null, null, json);
    }

    @Test
    public void testRemoveGlutenParamsIfNeed() throws Exception {
        val sparkPrefix = "kylin.engine.spark-conf.";
        val config = getTestConfig();
        config.setProperty("kylin.env", "PROD");
        config.setProperty(sparkPrefix + SPARK_PLUGINS, GLUTEN_PLUGIN + ",org.apache.spark.kyuubi.KyuubiPlugin");
        config.setProperty(sparkPrefix + "spark.gluten.enable", "true");
        config.setProperty(sparkPrefix + "spark.master", "yarn");
        config.setProperty(sparkPrefix + "spark.eventLog.enabled", "false");
        config.setProperty("kylin.engine.gluten.enabled", "true");
        val application = new SparkApplication() {
            @Override
            protected void doExecute() {
                // do nothing
            }
        };
        application.className = SegmentBuildJob.class.getName();
        application.config = config;
        assertWithGluten(application);

        config.setProperty("kylin.engine.gluten.enabled", "false");
        assertWithOutGluten(application);

        application.className = InternalTableLoadJob.class.getName();
        assertWithGluten(application);

        application.className = SegmentBuildJob.class.getName();
        assertWithOutGluten(application);
    }

    private static void assertWithGluten(SparkApplication application) throws Exception {
        val sparkConf = new SparkConf();
        sparkConf.set("spark.master", "yarn");
        sparkConf.set("spark.eventLog.enabled", "false");
        application.exchangeSparkConf(sparkConf);
        val atomicSparkConf = ((AtomicReference<SparkConf>) ReflectionTestUtils.getField(application,
                "atomicSparkConf"));
        val actalSparkConf = atomicSparkConf.get();
        assertEquals(COLUMNAR_SHUFFLE_MANAGER, actalSparkConf.get(SPARK_SHUFFLE_MANAGER));
        assertEquals("true", actalSparkConf.get("spark.gluten.enable"));
        assertEquals(GLUTEN_PLUGIN + ",org.apache.spark.kyuubi.KyuubiPlugin", actalSparkConf.get(SPARK_PLUGINS));
        assertEquals("yarn", actalSparkConf.get("spark.master"));
        assertEquals("false", actalSparkConf.get("spark.eventLog.enabled"));
    }

    private static void assertWithOutGluten(SparkApplication application) throws Exception {
        val sparkConf = new SparkConf();
        sparkConf.set("spark.master", "yarn");
        sparkConf.set("spark.eventLog.enabled", "false");
        application.exchangeSparkConf(sparkConf);
        val atomicSparkConf = ((AtomicReference<SparkConf>) ReflectionTestUtils.getField(application,
                "atomicSparkConf"));
        val actalSparkConf = atomicSparkConf.get();
        assertFalse(Arrays.stream(actalSparkConf.getAll()).anyMatch(conf -> conf._1.contains("gluten")));
        assertEquals("sort", actalSparkConf.get(SPARK_SHUFFLE_MANAGER));
        assertEquals("org.apache.spark.kyuubi.KyuubiPlugin", actalSparkConf.get(SPARK_PLUGINS));
    }

    @Test
    public void testDisableCurrentThreadGlutenIfNeed() throws Exception {
        val config = getTestConfig();
        config.setProperty("kylin.engine.gluten.enabled", "true");
        val application = new SparkApplication() {
            @Override
            protected void doExecute() {
                // do nothing
            }
        };
        application.className = SegmentBuildJob.class.getName();
        application.config = config;
        val sparkConf = new SparkConf();
        sparkConf.set("spark.master", "local[111]");
        assertNullEnableForCurrentThread(application, sparkConf);

        config.setProperty("kylin.engine.gluten.enabled", "false");
        assertFalseEnableForCurrentThread(application, sparkConf);

        application.className = InternalTableLoadJob.class.getName();
        assertNullEnableForCurrentThread(application, sparkConf);
    }

    private void assertNullEnableForCurrentThread(SparkApplication application, SparkConf sparkConf) {
        try (val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate();) {
            application.ss = sparkSession;

            application.disableCurrentThreadGlutenIfNeed();
            assertNull(ss.sparkContext().getLocalProperty("gluten.enabledForCurrentThread"));
        }
    }

    private void assertFalseEnableForCurrentThread(SparkApplication application, SparkConf sparkConf) {
        try (val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate();) {
            application.ss = sparkSession;

            application.disableCurrentThreadGlutenIfNeed();
            assertEquals("false", application.ss.sparkContext().getLocalProperty("gluten.enabledForCurrentThread"));
        }
    }
}
