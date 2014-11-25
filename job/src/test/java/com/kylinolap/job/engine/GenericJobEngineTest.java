/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.engine;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.AbstractKylinTestCase;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.common.util.SSHClient;
import com.kylinolap.cube.CubeBuildTypeEnum;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.cube.project.ProjectManager;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.JobManager;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.constant.JobStepCmdTypeEnum;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.exception.InvalidJobInstanceException;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author ysong1
 */
public class GenericJobEngineTest {
    private static String cubeName = "test_kylin_cube_with_slr_empty";

    private static String tempTestMetadataUrl = "../examples/sample_metadata";
    private static JobManager jobManager;

    private static JobDAO jobDAO;

    private static String mrInputDir = "/tmp/mapredsmokeinput";
    private static String mrOutputDir1 = "/tmp/mapredsmokeoutput1";
    private static String mrOutputDir2 = "/tmp/mapredsmokeoutput2";
    private static String mrCmd = "hadoop --config /etc/hadoop/conf jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples-2.*.jar wordcount " + mrInputDir + " ";

    public static void removeHdfsDir(SSHClient hadoopCli, String hdfsDir) throws Exception {
        String cmd = "hadoop fs -rm -f -r " + hdfsDir;
        hadoopCli.execCommand(cmd);
    }

    public static String getHadoopCliHostname() {
        return getHadoopCliAsRemote() ? KylinConfig.getInstanceFromEnv().getRemoteHadoopCliHostname() : null;
    }

    public static String getHadoopCliUsername() {
        return getHadoopCliAsRemote() ? KylinConfig.getInstanceFromEnv().getRemoteHadoopCliUsername() : null;
    }

    public static String getHadoopCliPassword() {
        return getHadoopCliAsRemote() ? KylinConfig.getInstanceFromEnv().getRemoteHadoopCliPassword() : null;
    }

    public static boolean getHadoopCliAsRemote() {
        return KylinConfig.getInstanceFromEnv().getRunAsRemoteCommand();
    }

    public static void scpFilesToHdfs(SSHClient hadoopCli, String[] localFiles, String hdfsDir) throws Exception {
        String remoteTempDir = "/tmp/";

        List<String> nameList = new ArrayList<String>();
        for (int i = 0; i < localFiles.length; i++) {
            File file = new File(localFiles[i]);
            String filename = file.getName();
            nameList.add(filename);
        }
        for (String f : localFiles) {
            hadoopCli.scpFileToRemote(f, remoteTempDir);
        }
        for (String f : nameList) {
            hadoopCli.execCommand("hadoop fs -mkdir -p " + hdfsDir);
            hadoopCli.execCommand("hadoop fs -put -f " + remoteTempDir + f + " " + hdfsDir + "/" + f);
            hadoopCli.execCommand("hadoop fs -chmod 777 " + hdfsDir + "/" + f);
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {

        FileUtils.forceMkdir(new File(KylinConfig.getInstanceFromEnv().getKylinJobLogDir()));

        FileUtils.deleteDirectory(new File(tempTestMetadataUrl));
        FileUtils.copyDirectory(new File(AbstractKylinTestCase.LOCALMETA_TEST_DATA), new File(tempTestMetadataUrl));
        System.setProperty(KylinConfig.KYLIN_CONF, tempTestMetadataUrl);

        // deploy files to hdfs
        SSHClient hadoopCli = new SSHClient(getHadoopCliHostname(), getHadoopCliUsername(), getHadoopCliPassword(), null);
        scpFilesToHdfs(hadoopCli, new String[] { "src/test/resources/json/dummy_jobinstance.json" }, mrInputDir);
        // deploy sample java jar
        hadoopCli.scpFileToRemote("src/test/resources/jarfile/SampleJavaProgram.jarfile", "/tmp");
        hadoopCli.scpFileToRemote("src/test/resources/jarfile/SampleBadJavaProgram.jarfile", "/tmp");

        // create log dir
        hadoopCli.execCommand("mkdir -p " + KylinConfig.getInstanceFromEnv().getKylinJobLogDir());
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setMetadataUrl(tempTestMetadataUrl);

        jobManager = new JobManager("GenericJobEngineTest", new JobEngineConfig(KylinConfig.getInstanceFromEnv()));

        jobDAO = JobDAO.getInstance(KylinConfig.getInstanceFromEnv());

        jobDAO.updateJobInstance(createARunningJobInstance("a_running_job"));

        jobManager.startJobEngine(2);
        Thread.sleep(2000);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        FileUtils.deleteDirectory(new File(tempTestMetadataUrl));
        System.clearProperty(KylinConfig.KYLIN_CONF);

        // print metrics
        System.out.println("Job step duration seconds 80 percentile: " + jobManager.getPercentileJobStepDuration(80));
        System.out.println("Max job step duration seconds: " + jobManager.getMaxJobStepDuration());
        System.out.println("Min job step duration seconds: " + jobManager.getMinJobStepDuration());
        System.out.println("# job steps executed: " + jobManager.getNumberOfJobStepsExecuted());
        System.out.println("Engine ID: " + jobManager.getPrimaryEngineID());

        jobManager.stopJobEngine();

    }

    @Before
    public void before() throws Exception {
        SSHClient hadoopCli = new SSHClient(getHadoopCliHostname(), getHadoopCliUsername(), getHadoopCliPassword(), null);
        removeHdfsDir(hadoopCli, mrOutputDir1);
        removeHdfsDir(hadoopCli, mrOutputDir2);

        MetadataManager.removeInstance(KylinConfig.getInstanceFromEnv());
        CubeManager.removeInstance(KylinConfig.getInstanceFromEnv());
        ProjectManager.removeInstance(KylinConfig.getInstanceFromEnv());
    }

    @Test(expected = InvalidJobInstanceException.class)
    public void testSubmitDuplicatedJobs() throws IOException, InvalidJobInstanceException, CubeIntegrityException {
        String uuid = "bad_job_2";
        JobInstance job = createASingleStepBadJobInstance(uuid);
        // job.setStatus(JobStatusEnum.KILLED);
        jobManager.submitJob(job);
        jobManager.submitJob(job);
    }

    @Test
    public void testGoodJob() throws Exception {
        String uuid = "good_job_uuid";
        jobManager.submitJob(createAGoodJobInstance(uuid, 5));

        waitUntilJobComplete(uuid);

        JobInstance savedJob1 = jobManager.getJob(uuid);
        assertEquals(JobStatusEnum.FINISHED, savedJob1.getStatus());
        String jobString = JsonUtil.writeValueAsIndentString(savedJob1);
        System.out.println(jobString);
        assertTrue(jobString.length() > 0);

        // cube should be updated
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        String cubeString = JsonUtil.writeValueAsIndentString(cube);
        System.out.println(cubeString);
        assertEquals("good_job_uuid", cube.getSegments().get(0).getLastBuildJobID());
        assertTrue(cube.getSegments().get(0).getSizeKB() > 0);
    }

    @Test
    public void testSingleStepBadJob() throws Exception {
        String uuid = "single_step_bad_job_uuid";
        jobManager.submitJob(createASingleStepBadJobInstance(uuid));

        waitUntilJobComplete(uuid);

        JobInstance savedJob1 = jobManager.getJob(uuid);
        assertEquals(JobStatusEnum.ERROR, savedJob1.getStatus());
        String jobString = JsonUtil.writeValueAsIndentString(savedJob1);
        System.out.println(jobString);
        assertTrue(jobString.length() > 0);

        // cube should be updated
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        String cubeString = JsonUtil.writeValueAsIndentString(cube);
        System.out.println(cubeString);
        assertEquals(CubeSegmentStatusEnum.NEW, cube.getSegments().get(0).getStatus());
    }

    @Test
    public void testSelfCheckAndResume() throws Exception {
        String uuid = "a_running_job";
        assertEquals(JobStatusEnum.ERROR, jobManager.getJob(uuid).getStatus());
        // check step status
        assertEquals(JobStepStatusEnum.FINISHED, jobManager.getJob(uuid).getSteps().get(0).getStatus());
        assertEquals(JobStepStatusEnum.ERROR, jobManager.getJob(uuid).getSteps().get(1).getStatus());
        assertEquals(JobStepStatusEnum.PENDING, jobManager.getJob(uuid).getSteps().get(2).getStatus());

        jobManager.resumeJob(uuid);

        waitUntilJobComplete(uuid);
        assertEquals(JobStatusEnum.FINISHED, jobManager.getJob(uuid).getStatus());
    }

    @Test
    public void testDiscardSyncStep() throws Exception {
        String uuid = "a_long_running_good_job_uuid";
        JobInstance job = createAGoodJobInstance(uuid, 600);
        jobManager.submitJob(job);

        try {
            // sleep for 5 seconds
            Thread.sleep(5L * 1000L);
        } catch (Exception e) {
        }

        try {
            jobManager.discardJob(uuid);
        } catch (RuntimeException e) {
            throw e;
        }

        waitUntilJobComplete(uuid);

        JobInstance killedJob = jobManager.getJob(uuid);
        assertEquals(JobStepStatusEnum.DISCARDED, killedJob.getSteps().get(0).getStatus());
        assertEquals(JobStatusEnum.DISCARDED, killedJob.getStatus());
    }

    @Test
    public void testKillMrStep() throws Exception {
        String uuid = "a_long_running_good_job_uuid_2";
        JobInstance job = createAGoodJobInstance(uuid, 1);
        jobManager.submitJob(job);

        try {
            waitUntilMrStepIsRunning(uuid);
            jobManager.discardJob(uuid);
        } catch (RuntimeException e) {
            throw e;
        }

        waitUntilJobComplete(uuid);

        JobInstance killedJob = jobManager.getJob(uuid);
        assertEquals(JobStepStatusEnum.ERROR, killedJob.getSteps().get(1).getStatus());
        assertEquals(JobStatusEnum.ERROR, killedJob.getStatus());

        // cube should be updated
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        String cubeString = JsonUtil.writeValueAsIndentString(cube);
        System.out.println(cubeString);
        assertEquals(0, cube.getSegments().size());
    }

    private void waitUntilMrStepIsRunning(String jobUuid) throws InterruptedException, IOException {
        boolean running = false;
        while (running == false) {
            // sleep for 1 seconds
            Thread.sleep(1 * 1000L);

            JobInstance savedJob = jobManager.getJob(jobUuid);
            for (JobStep step : savedJob.getSteps()) {
                if (step.getCmdType().equals(JobStepCmdTypeEnum.SHELL_CMD_HADOOP) && step.getStatus().equals(JobStepStatusEnum.RUNNING) && step.getInfo(JobInstance.MR_JOB_ID) != null) {
                    System.out.println("MR step is running with id " + step.getInfo(JobInstance.MR_JOB_ID));
                    running = true;
                    break;
                }
            }

        }

    }

    private void waitUntilJobComplete(String jobUuid) throws IOException, InterruptedException {
        boolean finished = false;
        while (!finished) {
            // sleep for 5 seconds
            Thread.sleep(5 * 1000L);

            finished = true;

            JobInstance savedJob = jobManager.getJob(jobUuid);
            JobStatusEnum jobStatus = savedJob.getStatus();
            System.out.println("Job " + jobUuid + " status: " + jobStatus);
            if (jobStatus.getCode() <= JobStatusEnum.RUNNING.getCode()) {
                finished = false;
            }
        }
    }

    private JobInstance createAGoodJobInstance(String uuid, int syncCmdSleepSeconds) throws IOException, CubeIntegrityException {
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        cube.getSegments().clear();
        CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).updateCube(cube);
        CubeSegment seg = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).allocateSegments(cube, CubeBuildTypeEnum.BUILD, 0, 12345L).get(0);

        JobInstance jobInstance = new JobInstance();
        jobInstance.setUuid(uuid);
        jobInstance.setRelatedCube(cubeName);
        jobInstance.setRelatedSegment(seg.getName());
        jobInstance.setName("A_Good_Job");
        // jobInstance.setStatus(JobStatusEnum.PENDING);
        jobInstance.setType(CubeBuildTypeEnum.BUILD);
        // jobInstance.putInputParameter(JobConstants.PROP_STORAGE_LOCATION,
        // "htablename");

        JobStep step1 = new JobStep();
        step1.setName("step1");
        step1.setExecCmd("java -jar /tmp/SampleJavaProgram.jarfile " + syncCmdSleepSeconds);
        step1.setStatus(JobStepStatusEnum.PENDING);
        step1.setSequenceID(0);
        step1.setCmdType(JobStepCmdTypeEnum.SHELL_CMD);
        step1.setRunAsync(false);

        // async mr step
        JobStep step2 = new JobStep();
        step2.setName(JobConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE);
        step2.setExecCmd(mrCmd + mrOutputDir1);
        step2.setStatus(JobStepStatusEnum.PENDING);
        step2.setSequenceID(1);
        step2.setCmdType(JobStepCmdTypeEnum.SHELL_CMD_HADOOP);
        step2.setRunAsync(true);

        // async mr step
        JobStep step3 = new JobStep();
        step3.setName("synced mr step");
        step3.setExecCmd(mrCmd + mrOutputDir2);
        step3.setStatus(JobStepStatusEnum.PENDING);
        step3.setSequenceID(2);
        step3.setCmdType(JobStepCmdTypeEnum.SHELL_CMD_HADOOP);
        step3.setRunAsync(false);

        jobInstance.addStep(0, step1);
        jobInstance.addStep(1, step2);
        jobInstance.addStep(2, step3);
        return jobInstance;
    }

    private JobInstance createASingleStepBadJobInstance(String uuid) throws IOException, CubeIntegrityException {
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        cube.getSegments().clear();
        CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).updateCube(cube);
        CubeSegment seg = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).allocateSegments(cube, CubeBuildTypeEnum.BUILD, 0, 12345L).get(0);

        JobInstance jobInstance = new JobInstance();
        jobInstance.setUuid(uuid);
        jobInstance.setRelatedCube(cubeName);
        jobInstance.setRelatedSegment(seg.getName());
        jobInstance.setName("A_Bad_Job");
        // jobInstance.setStatus(JobStatusEnum.PENDING);
        jobInstance.setType(CubeBuildTypeEnum.BUILD);
        // jobInstance.putInputParameter(JobConstants.PROP_STORAGE_LOCATION,
        // "htablename");

        JobStep step1 = new JobStep();
        step1.setName("step1");
        step1.setExecCmd("java -jar /tmp/SampleBadJavaProgram.jarfile");
        step1.setStatus(JobStepStatusEnum.PENDING);
        step1.setSequenceID(0);
        step1.setRunAsync(false);
        step1.setCmdType(JobStepCmdTypeEnum.SHELL_CMD);
        jobInstance.addStep(0, step1);

        return jobInstance;
    }

    private static JobInstance createARunningJobInstance(String uuid) throws IOException, CubeIntegrityException {
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        cube.getSegments().clear();
        CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).updateCube(cube);
        CubeSegment seg = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).allocateSegments(cube, CubeBuildTypeEnum.BUILD, 0, 12345L).get(0);

        JobInstance jobInstance = new JobInstance();
        jobInstance.setUuid(uuid);
        jobInstance.setRelatedCube(cubeName);
        jobInstance.setRelatedSegment(seg.getName());
        jobInstance.setName("A_Running_Job");
        // jobInstance.setStatus(JobStatusEnum.RUNNING);
        jobInstance.setType(CubeBuildTypeEnum.BUILD);
        // jobInstance.putInputParameter(JobConstants.PROP_STORAGE_LOCATION,
        // "htablename");

        JobStep step1 = new JobStep();
        step1.setName("step1");
        step1.setExecCmd("echo step1");
        step1.setStatus(JobStepStatusEnum.FINISHED);
        step1.setSequenceID(0);
        step1.setRunAsync(false);
        step1.setCmdType(JobStepCmdTypeEnum.SHELL_CMD);

        JobStep step2 = new JobStep();
        step2.setName("step2");
        step2.setExecCmd(mrCmd + mrOutputDir1);
        step2.setStatus(JobStepStatusEnum.RUNNING);
        step2.setSequenceID(1);
        step2.setRunAsync(true);
        step2.setCmdType(JobStepCmdTypeEnum.SHELL_CMD_HADOOP);

        JobStep step3 = new JobStep();
        step3.setName("step3");
        step3.setExecCmd("java -jar /tmp/SampleJavaProgram.jarfile 3");
        step3.setStatus(JobStepStatusEnum.PENDING);
        step3.setSequenceID(2);
        step3.setRunAsync(false);
        step3.setCmdType(JobStepCmdTypeEnum.SHELL_CMD);

        jobInstance.addStep(0, step1);
        jobInstance.addStep(1, step2);
        jobInstance.addStep(2, step3);
        return jobInstance;
    }
}
