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
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class JobInstanceExtractor extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(JobInstanceExtractor.class);

    private static final int DEFAULT_PERIOD = 3;

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify jobs in which project to extract").create("project");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(false).withDescription("Specify jobs related to which cube to extract").create("cube");
    @SuppressWarnings("static-access")
    private static final Option OPTION_PERIOD = OptionBuilder.withArgName("period").hasArg().isRequired(false).withDescription("specify how many days of kylin jobs to extract. Default " + DEFAULT_PERIOD + ".").create("period");

    KylinConfig config;
    ProjectManager projectManager;
    ExecutableManager executableManager;

    public JobInstanceExtractor() {
        config = KylinConfig.getInstanceFromEnv();
        executableManager = ExecutableManager.getInstance(config);
        projectManager = ProjectManager.getInstance(config);

        packageType = "jobInstances";

        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_CUBE);
        options.addOption(OPTION_PERIOD);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        String cube = optionsHelper.hasOption(OPTION_CUBE) ? optionsHelper.getOptionValue(OPTION_CUBE) : null;
        String project = optionsHelper.hasOption(OPTION_PROJECT) ? optionsHelper.getOptionValue(OPTION_PROJECT) : null;
        int period = optionsHelper.hasOption(OPTION_PERIOD) ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_PERIOD)) : DEFAULT_PERIOD;

        long endTime = System.currentTimeMillis();
        long startTime = endTime - period * 24 * 3600 * 1000; // time in Millis
        List<JobInstance> jobInstances = listJobInstances(cube, project, startTime, endTime);
        logger.info("There are {} jobInstances to extract.", jobInstances.size());

        ObjectMapper mapper = new ObjectMapper();
        for (JobInstance jobInstance : jobInstances) {
            mapper.writeValue(new File(exportDir, jobInstance.getUuid() + ".json"), jobInstance);
        }
    }

    private List<JobInstance> listJobInstances(String project, String cube, long startTime, long endTime) {
        final List<JobInstance> result = Lists.newArrayList();
        final List<AbstractExecutable> executables = executableManager.getAllExecutables(startTime, endTime);
        final Map<String, Output> allOutputs = executableManager.getAllOutputs();
        for (AbstractExecutable executable : executables) {
            if (executable instanceof CubingJob) {
                String cubeName = CubingExecutableUtil.getCubeName(executable.getParams());
                boolean shouldExtract = false;
                if (cube == null || cube.equalsIgnoreCase(cubeName)) {
                    if (project == null) {
                        shouldExtract = true;
                    } else {
                        ProjectInstance projectInstance = projectManager.getProject(project);
                        if (projectInstance != null && projectInstance.containsRealization(RealizationType.CUBE, cubeName)) {
                            shouldExtract = true;
                        }
                    }
                }

                if (shouldExtract) {
                    result.add(parseToJobInstance((CubingJob) executable, allOutputs));
                }
            }
        }
        return result;
    }

    private JobInstance parseToJobInstance(CubingJob cubeJob, Map<String, Output> outputs) {
        Output output = outputs.get(cubeJob.getId());
        final JobInstance result = new JobInstance();
        result.setName(cubeJob.getName());
        result.setRelatedCube(CubingExecutableUtil.getCubeName(cubeJob.getParams()));
        result.setRelatedSegment(CubingExecutableUtil.getSegmentId(cubeJob.getParams()));
        result.setLastModified(output.getLastModified());
        result.setSubmitter(cubeJob.getSubmitter());
        result.setUuid(cubeJob.getId());
        result.setType(CubeBuildTypeEnum.BUILD);
        result.setStatus(parseToJobStatus(output.getState()));
        result.setMrWaiting(AbstractExecutable.getExtraInfoAsLong(output, CubingJob.MAP_REDUCE_WAIT_TIME, 0L) / 1000);
        result.setExecStartTime(AbstractExecutable.getStartTime(output));
        result.setExecEndTime(AbstractExecutable.getEndTime(output));
        result.setDuration(AbstractExecutable.getDuration(AbstractExecutable.getStartTime(output), AbstractExecutable.getEndTime(output)) / 1000);
        for (int i = 0; i < cubeJob.getTasks().size(); ++i) {
            AbstractExecutable task = cubeJob.getTasks().get(i);
            result.addStep(parseToJobStep(task, i, outputs.get(task.getId())));
        }
        return result;
    }

    private JobStatusEnum parseToJobStatus(ExecutableState state) {
        switch (state) {
        case READY:
            return JobStatusEnum.PENDING;
        case RUNNING:
            return JobStatusEnum.RUNNING;
        case ERROR:
            return JobStatusEnum.ERROR;
        case DISCARDED:
            return JobStatusEnum.DISCARDED;
        case SUCCEED:
            return JobStatusEnum.FINISHED;
        case STOPPED:
        default:
            throw new RuntimeException("invalid state:" + state);
        }
    }

    private JobInstance.JobStep parseToJobStep(AbstractExecutable task, int i, Output stepOutput) {
        Preconditions.checkNotNull(stepOutput);
        JobInstance.JobStep result = new JobInstance.JobStep();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(i);
        result.setStatus(parseToJobStepStatus(stepOutput.getState()));
        for (Map.Entry<String, String> entry : stepOutput.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        result.setExecStartTime(AbstractExecutable.getStartTime(stepOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stepOutput));
        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        if (task instanceof MapReduceExecutable) {
            result.setExecCmd(((MapReduceExecutable) task).getMapReduceParams());
            result.setExecWaitTime(AbstractExecutable.getExtraInfoAsLong(stepOutput, MapReduceExecutable.MAP_REDUCE_WAIT_TIME, 0L) / 1000);
        }
        if (task instanceof HadoopShellExecutable) {
            result.setExecCmd(((HadoopShellExecutable) task).getJobParams());
        }
        return result;
    }

    private JobStepStatusEnum parseToJobStepStatus(ExecutableState state) {
        switch (state) {
        case READY:
            return JobStepStatusEnum.PENDING;
        case RUNNING:
            return JobStepStatusEnum.RUNNING;
        case ERROR:
            return JobStepStatusEnum.ERROR;
        case DISCARDED:
            return JobStepStatusEnum.DISCARDED;
        case SUCCEED:
            return JobStepStatusEnum.FINISHED;
        case STOPPED:
        default:
            throw new RuntimeException("invalid state:" + state);
        }
    }
}
