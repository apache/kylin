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

import com.google.common.base.Strings;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.util.JobRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CubeBuildingCLIV2 extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(CubeBuildingCLIV2.class);

    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(true)
            .withDescription("Specify for which cube to build").create("cube");
    private static final Option OPTION_BUILD_TYPE = OptionBuilder.withArgName("buildType").hasArg().isRequired(false)
            .withDescription("Specify for the build type").create("buildType");
    private static final Option OPTION_TIME_START = OptionBuilder.withArgName("startTime").hasArg().isRequired(false)
            .withDescription("Specify the start time of the segment").create("startTime");
    private static final Option OPTION_TIME_END = OptionBuilder.withArgName("endTime").hasArg().isRequired(true)
            .withDescription("Specify the end time of the segment").create("endTime");
    private static final Option OPTION_HOST = OptionBuilder.withArgName("host").hasArg().isRequired(true)
            .withDescription("Specify the kylin server host").create("host");
    private static final Option OPTION_PORT = OptionBuilder.withArgName("port").hasArg().isRequired(true)
            .withDescription("Specify the kylin server port").create("port");
    private static final Option OPTION_USER_NAME = OptionBuilder.withArgName("userName").hasArg().isRequired(true)
            .withDescription("Specify the kylin server user name").create("userName");
    private static final Option OPTION_PASSWORD= OptionBuilder.withArgName("password").hasArg().isRequired(true)
            .withDescription("Specify the kylin server password").create("password");
    private static final Option OPTION_WAITING_FOR_END = OptionBuilder.withArgName("waitingForEnd").hasArg().isRequired(false)
            .withDescription("Specify whether waiting for end").create("waitingForEnd");
    private static final Option OPTION_RETRY_NUMBER = OptionBuilder.withArgName("retryNumber").hasArg().isRequired(false)
            .withDescription("Specify retry number when execute failed").create("retryNumber");
    private static final Option OPTION_DISCARD_ERROR_JOB = OptionBuilder.withArgName("discardErrorJob").hasArg().isRequired(false)
            .withDescription("Specify discard job when execute failed").create("discardErrorJob");

    private final Options options;

    public CubeBuildingCLIV2() {
        options = new Options();
        options.addOption(OPTION_CUBE);
        options.addOption(OPTION_BUILD_TYPE);
        options.addOption(OPTION_TIME_START);
        options.addOption(OPTION_TIME_END);
        options.addOption(OPTION_HOST);
        options.addOption(OPTION_PORT);
        options.addOption(OPTION_USER_NAME);
        options.addOption(OPTION_PASSWORD);
        options.addOption(OPTION_WAITING_FOR_END);
        options.addOption(OPTION_RETRY_NUMBER);
        options.addOption(OPTION_DISCARD_ERROR_JOB);
    }

    protected Options getOptions() {
        return options;
    }

    protected void execute(OptionsHelper optionsHelper) throws IOException {
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
        String host = optionsHelper.getOptionValue(OPTION_HOST);
        Integer port = Integer.parseInt(optionsHelper.getOptionValue(OPTION_PORT));

        String userName = optionsHelper.getOptionValue(OPTION_USER_NAME);
        String password = optionsHelper.getOptionValue(OPTION_PASSWORD);

        Boolean waitingForEnd = true;
        if (!Strings.isNullOrEmpty(optionsHelper.getOptionValue(OPTION_WAITING_FOR_END))) {
            waitingForEnd = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_WAITING_FOR_END));
        }
        JobRestClient client = new JobRestClient(host, port, userName, password);
        System.out.println("start building cube.");
        JobInstance jobInstance = submitJob(client, cubeName, startTime, endTime, buildType);
        if (waitingForEnd) {
            int retryNumber = 0;
            if (!Strings.isNullOrEmpty(optionsHelper.getOptionValue(OPTION_RETRY_NUMBER))) {
                retryNumber = Integer.parseInt(optionsHelper.getOptionValue(OPTION_RETRY_NUMBER));
            }
            while (!jobInstance.getStatus().isComplete()) {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    System.err.println("Thread interrupted, exit");
                    System.exit(-1);
                }
                jobInstance = client.getJobStatus(jobInstance.getId());
                System.out.println("job " + jobInstance.getId() + " get status : " + jobInstance.getStatus());
                if (jobInstance.getStatus().equals(JobStatusEnum.ERROR) && retryNumber > 0) {
                    System.out.println("retry count is " + retryNumber);
                    retryNumber--;
                    jobInstance = client.resumeJob(jobInstance.getId());
                }
            }
            if (!jobInstance.getStatus().equals(JobStatusEnum.FINISHED)) {
                boolean discardErrorJob = false;
                if (!Strings.isNullOrEmpty(optionsHelper.getOptionValue(OPTION_DISCARD_ERROR_JOB))) {
                    discardErrorJob = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_DISCARD_ERROR_JOB));
                }
                if (discardErrorJob) {
                    client.discardJob(jobInstance.getId());
                }
                System.exit(-1);
            }

        }
    }

    private JobInstance submitJob(JobRestClient client, String cubeName, long startDate, long endDate, String buildType) throws IOException {
        CubeBuildTypeEnum buildTypeEnum = CubeBuildTypeEnum.valueOf(buildType);
        JobInstance jobInstance = client.buildCubeV2(cubeName, startDate, endDate, buildTypeEnum);
        System.out.println("building cube job:");
        System.out.println(client.JobInstance2JsonString(jobInstance));
        return jobInstance;
    }


    public static void main(String[] args) {
        CubeBuildingCLIV2 cli = new CubeBuildingCLIV2();
        try {
            cli.execute(args);
            System.exit(0);
        } catch (Exception e) {
            logger.error("error running cube building", e);
            System.exit(-1);
        }
    }
}
