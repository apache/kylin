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

package org.apache.kylin.rest.job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class KylinHealthCheckJob extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(KylinHealthCheckJob.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_FIX = OptionBuilder.withArgName("fix").hasArg().isRequired(false)
            .withDescription("Fix the unhealthy cube").create("fix");

    public static void main(String[] args) throws Exception {
        new KylinHealthCheckJob().execute(args);
    }

    final KylinConfig config;
    final BufferedLogger reporter = new BufferedLogger(logger);
    final CubeManager cubeManager;

    public KylinHealthCheckJob() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public KylinHealthCheckJob(KylinConfig config) {
        this.config = config;
        this.cubeManager = CubeManager.getInstance(config);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        // TODO: Support to fix the unhealthy cube automatically
        options.addOption(OPTION_FIX);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("options: '" + optionsHelper.getOptionsAsString() + "'");
        checkCubeHealth();
    }

    private void checkCubeHealth() throws Exception {
        CubeManager cubeManager = CubeManager.getInstance(config);

        List<CubeInstance> cubes = cubeManager.listAllCubes();
        checkErrorMeta();

        // Check if the cubeid data exist for later cube merge
        checkSegmentHDFSPath(cubes);

        // Check if the hbase table exits or online
        checkHBaseTables(cubes);

        // Check if there are holes in cube
        // TODO: check if there are overlaps in segments of cube
        checkCubeHoles(cubes);

        // Check if there are too many segments
        checkTooManySegments(cubes);

        // Check if there are stale metadata
        checkStaleSegments(cubes);

        // Disable/Delete the out-of-date cube
        checkOutOfDateCube(cubes);

        // Check data expand rate
        checkDataExpansionRate(cubes);

        // Check auto merge param
        checkCubeDescParams(cubes);

        // ERROR history stopped build job
        checkStoppedJob();

        sendMail(reporter.getBufferedLog());
    }

    private void sendMail(String content) {
        logger.info("Send Kylin cluster report");
        String subject = "Kylin Cluster Health Report of " + config.getClusterName() + " on "
                + new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT).format(new Date());
        List<String> users = Lists.newArrayList(config.getAdminDls());
        new MailService(config).sendMail(users, subject, content, false);
    }

    private void checkErrorMeta() {
        reporter.log("## Checking metadata");

        CubeManager cubeManager = CubeManager.getInstance(config);
        for (String cube : cubeManager.getErrorCubes()) {
            reporter.log("Error loading CubeDesc at " + cube);
        }

        DataModelManager modelManager = DataModelManager.getInstance(config);
        for (String model : modelManager.getErrorModels()) {
            reporter.log("Error loading DataModelDesc at " + model);
        }
    }

    private void checkStoppedJob() throws Exception {
        reporter.log("## Cleanup stopped job");
        int staleJobThresholdInDays = config.getStaleJobThresholdInDays();
        long outdatedJobTimeCut = System.currentTimeMillis() - 1L * staleJobThresholdInDays * 24 * 60 * 60 * 1000;
        ExecutableDao executableDao = ExecutableDao.getInstance(config);
        // discard all expired ERROR or STOPPED jobs
        List<ExecutablePO> allExecutable = executableDao.getJobs();
        for (ExecutablePO executable : allExecutable) {
            long lastModified = executable.getLastModified();
            String jobStatus = executableDao.getJobOutput(executable.getUuid()).getStatus();
            if (lastModified < outdatedJobTimeCut && (ExecutableState.ERROR.toString().equals(jobStatus)
                    || ExecutableState.STOPPED.toString().equals(jobStatus))) {
                // ExecutableManager.getInstance(config).discardJob(executable.getId());
                if (executable.getType().equals(CubingJob.class.getName())
                        || executable.getType().equals(CheckpointExecutable.class.getName())) {
                    reporter.log("Should discard job: {}, which in ERROR/STOPPED state for {} days", executable.getId(),
                            staleJobThresholdInDays);
                } else {
                    logger.warn("Unknown out of date job: {} with type: {}, which in ERROR/STOPPED state for {} days",
                            executable.getId(), executable.getType(), staleJobThresholdInDays);
                }
            }
        }
    }

    private void checkSegmentHDFSPath(List<CubeInstance> cubes) throws IOException {
        reporter.log("## Fix missing HDFS path of segments");
        FileSystem defaultFs = HadoopUtil.getWorkingFileSystem();
        for (CubeInstance cube : cubes) {
            for (CubeSegment segment : cube.getSegments()) {
                String jobUuid = segment.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String path = JobBuilderSupport.getJobWorkingDir(config.getHdfsWorkingDirectory(), jobUuid);
                    if (!defaultFs.exists(new Path(path))) {
                        reporter.log(
                                "Project: {} cube: {} segment: {} cube id data: {} don't exist and need to rebuild it",
                                cube.getProject(), cube.getName(), segment, path);
                        reporter.log(
                                "The rebuild url: -d '{\"startTime\":{}, \"endTime\":{}, \"buildType\":\"REFRESH\"}' /kylin/api/cubes/{}/build",
                                segment.getTSRange().start, segment.getTSRange().end, cube.getName());
                    }
                }
            }
        }
    }

    private void checkHBaseTables(List<CubeInstance> cubes) throws IOException {
        reporter.log("## Checking HBase Table of segments");
        HBaseAdmin hbaseAdmin = new HBaseAdmin(HBaseConfiguration.create());
        try {
            for (CubeInstance cube : cubes) {
                for (CubeSegment segment : cube.getSegments()) {
                    if (segment.getStatus() != SegmentStatusEnum.NEW) {
                        String tableName = segment.getStorageLocationIdentifier();
                        if ((!hbaseAdmin.tableExists(tableName)) || (!hbaseAdmin.isTableEnabled(tableName))) {
                            reporter.log("HBase table: {} not exist for segment: {}, project: {}", tableName, segment,
                                    cube.getProject());
                            reporter.log(
                                    "The rebuild url: -d '{\"startTime\":{}, \"endTime\":{}, \"buildType\":\"REFRESH\"}' /kylin/api/cubes/{}/build",
                                    segment.getTSRange().start, segment.getTSRange().end, cube.getName());
                        }
                    }
                }
            }
        } finally {
            if (null != hbaseAdmin) {
                hbaseAdmin.close();
            }
        }

    }

    private void checkCubeHoles(List<CubeInstance> cubes) {
        reporter.log("## Checking holes of Cubes");
        for (CubeInstance cube : cubes) {
            if (cube.isReady()) {
                List<CubeSegment> holes = cubeManager.calculateHoles(cube.getName());
                if (holes.size() > 0) {
                    reporter.log("{} holes in cube: {}, project: {}", holes.size(), cube.getName(), cube.getProject());
                }
            }
        }
    }

    private void checkTooManySegments(List<CubeInstance> cubes) {
        reporter.log("## Checking too many segments of Cubes");
        int warningSegmentNum = config.getWarningSegmentNum();
        if (warningSegmentNum < 0) {
            return;
        }
        for (CubeInstance cube : cubes) {
            if (cube.getSegments().size() >= warningSegmentNum) {
                reporter.log("Too many segments: {} for cube: {}, project: {}, please merge the segments",
                        cube.getSegments().size(), cube.getName(), cube.getProject());
            }
        }
    }

    private void checkStaleSegments(List<CubeInstance> cubes) {
        for (CubeInstance cube : cubes) {
            for (CubeSegment segment : cube.getSegments()) {
                if (segment.getInputRecordsSize() == 0) {
                    // TODO: add stale segment to report
                    logger.info("Segment: {} in project: {} may be stale", segment, cube.getProject());
                }
            }
        }
    }

    private void checkOutOfDateCube(List<CubeInstance> cubes) {
        reporter.log("## Checking out-of-date Cubes");
        int staleCubeThresholdInDays = config.getStaleCubeThresholdInDays();
        long outdatedCubeTimeCut = System.currentTimeMillis() - 1L * staleCubeThresholdInDays * 24 * 60 * 60 * 1000;
        for (CubeInstance cube : cubes) {
            long lastTime = cube.getLastModified();
            logger.info("Cube {} last modified time: {}, {}", cube.getName(), new Date(lastTime),
                    cube.getDescriptor().getNotifyList());
            if (lastTime < outdatedCubeTimeCut) {
                if (cube.isReady()) {
                    reporter.log(
                            "Ready Cube: {} in project: {} is not built more then {} days, maybe it can be disabled",
                            cube.getName(), cube.getProject(), staleCubeThresholdInDays);
                } else {
                    reporter.log(
                            "Disabled Cube: {} in project: {} is not built more then {} days, maybe it can be deleted",
                            cube.getName(), cube.getProject(), staleCubeThresholdInDays);
                }
            }
        }
    }

    private void checkDataExpansionRate(List<CubeInstance> cubes) {
        int warningExpansionRate = config.getWarningCubeExpansionRate();
        int expansionCheckMinCubeSizeInGb = config.getExpansionCheckMinCubeSizeInGb();
        for (CubeInstance cube : cubes) {
            long sizeRecordSize = cube.getInputRecordSizeBytes();
            if (sizeRecordSize > 0) {
                long cubeDataSize = cube.getSizeKB() * 1024;
                double expansionRate = (double) cubeDataSize / sizeRecordSize;
                if (sizeRecordSize > 1L * expansionCheckMinCubeSizeInGb * 1024 * 1024 * 1024) {
                    if (expansionRate > warningExpansionRate) {
                        logger.info("Cube: {} in project: {} with too large expansion rate: {}, cube data size: {}G",
                                cube.getName(), cube.getProject(), expansionRate, cubeDataSize / 1024 / 1024 / 1024);
                    }
                }
            }
        }
    }

    private void checkCubeDescParams(List<CubeInstance> cubes) {
        for (CubeInstance cube : cubes) {
            CubeDesc desc = cube.getDescriptor();
            long[] autoMergeTS = desc.getAutoMergeTimeRanges();
            if (autoMergeTS == null || autoMergeTS.length == 0) {
                logger.info("Cube: {} in project: {} with no auto merge params", cube.getName(), cube.getProject());
            }
            // long volatileRange = desc.getVolatileRange();
            long retentionRange = desc.getRetentionRange();
            if (retentionRange == 0) {
                logger.info("Cube: {} in project: {} with no retention params", cube.getName(), cube.getProject());
            }
            // queue params
        }
    }
}
