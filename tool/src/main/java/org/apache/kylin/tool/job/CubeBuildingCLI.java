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

package org.apache.kylin.tool.job;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.base.Strings;

public class CubeBuildingCLI extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(CubeBuildingCLI.class);

    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(true)
            .withDescription("Specify for which cube to build").create("cube");
    private static final Option OPTION_BUILD_TYPE = OptionBuilder.withArgName("buildType").hasArg().isRequired(false)
            .withDescription("Specify for the build type").create("buildType");
    private static final Option OPTION_TIME_START = OptionBuilder.withArgName("startTime").hasArg().isRequired(false)
            .withDescription("Specify the start time of the segment").create("startTime");
    private static final Option OPTION_TIME_END = OptionBuilder.withArgName("endTime").hasArg().isRequired(true)
            .withDescription("Specify the end time of the segment").create("endTime");

    private final Options options;

    private KylinConfig kylinConfig;
    private CubeManager cubeManager;
    private ExecutableManager executableManager;

    public CubeBuildingCLI() {
        options = new Options();
        options.addOption(OPTION_CUBE);
        options.addOption(OPTION_BUILD_TYPE);
        options.addOption(OPTION_TIME_START);
        options.addOption(OPTION_TIME_END);

        kylinConfig = KylinConfig.getInstanceFromEnv();
        cubeManager = CubeManager.getInstance(kylinConfig);
        executableManager = ExecutableManager.getInstance(kylinConfig);
    }

    protected Options getOptions() {
        return options;
    }

    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE);
        String buildType = optionsHelper.getOptionValue(OPTION_BUILD_TYPE);
        if (Strings.isNullOrEmpty(buildType)) {
            buildType = "BUILD";
        }
        Long startTime = 0L;
        if (!Strings.isNullOrEmpty(optionsHelper.getOptionValue(OPTION_TIME_START))) {
            startTime = Long.parseLong(optionsHelper.getOptionValue(OPTION_TIME_START));
        }
        Long endTime = Long.parseLong(optionsHelper.getOptionValue(OPTION_TIME_END));

        run(cubeName, startTime, endTime, buildType);
    }

    private void run(String cubeName, long startDate, long endDate, String buildType) throws IOException, JobException {
        CubeInstance cube = cubeManager.getCube(cubeName);
        Preconditions.checkArgument(cube != null, "Cube named " + cubeName + " does not exist!!!");
        CubeBuildTypeEnum buildTypeEnum = CubeBuildTypeEnum.valueOf(buildType);
        Preconditions.checkArgument(buildTypeEnum != null, "Build type named " + buildType + " does not exist!!!");
        submitJob(cube, new TSRange(startDate, endDate), buildTypeEnum, false, "SYSTEM");
    }

    private void submitJob(CubeInstance cube, TSRange tsRange, CubeBuildTypeEnum buildType,
            boolean forceMergeEmptySeg, String submitter) throws IOException, JobException {
        checkCubeDescSignature(cube);

        DefaultChainedExecutable job;

        if (buildType == CubeBuildTypeEnum.BUILD) {
            CubeSegment newSeg = cubeManager.appendSegment(cube, tsRange);
            job = EngineFactory.createBatchCubingJob(newSeg, submitter, null);
        } else if (buildType == CubeBuildTypeEnum.MERGE) {
            CubeSegment newSeg = cubeManager.mergeSegments(cube, tsRange, null, forceMergeEmptySeg);
            job = EngineFactory.createBatchMergeJob(newSeg, submitter);
        } else if (buildType == CubeBuildTypeEnum.REFRESH) {
            CubeSegment refreshSeg = cubeManager.refreshSegment(cube, tsRange, null);
            job = EngineFactory.createBatchCubingJob(refreshSeg, submitter, null);
        } else {
            throw new JobException("invalid build type:" + buildType);
        }
        executableManager.addJob(job);
    }

    private void checkCubeDescSignature(CubeInstance cube) {
        if (!cube.getDescriptor().checkSignature())
            throw new IllegalStateException("Inconsistent cube desc signature for " + cube.getDescriptor());
    }

    public static void main(String[] args) {
        CubeBuildingCLI cli = new CubeBuildingCLI();
        try {
            cli.execute(args);
            System.exit(0);
        } catch (Exception e) {
            logger.error("error start cube building", e);
            System.exit(-1);
        }
    }
}
