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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataCleanupJob extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(MetadataCleanupJob.class);
    private static final int DEFAULT_JOB_OUTDATED_THRESHOLD_DAYS = 30; // 30 days

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false)
            .withDescription("Delete the unused metadata").create("delete");

    @SuppressWarnings("static-access")
    private static final Option OPTION_THRESHOLD_FOR_JOB = OptionBuilder.withArgName("jobThreshold").hasArg()
            .isRequired(false).withDescription("Specify how many days of job metadata keeping. Default 30 days")
            .create("jobThreshold");

    public static void main(String[] args) throws Exception {
        new MetadataCleanupJob().execute(args);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DELETE);
        options.addOption(OPTION_THRESHOLD_FOR_JOB);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("delete option value: '" + optionsHelper.getOptionValue(OPTION_DELETE) + "'");
        logger.info("jobThreshold option value: '" + optionsHelper.getOptionValue(OPTION_THRESHOLD_FOR_JOB) + "'");
        boolean delete = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_DELETE));
        int jobOutdatedDays = optionsHelper.hasOption(OPTION_THRESHOLD_FOR_JOB)
                ? Integer.parseInt(optionsHelper.getOptionValue(OPTION_THRESHOLD_FOR_JOB))
                : DEFAULT_JOB_OUTDATED_THRESHOLD_DAYS;
        
        new org.apache.kylin.rest.job.MetadataCleanupJob().cleanup(delete, jobOutdatedDays);
    }
}
