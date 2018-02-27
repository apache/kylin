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

import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MetadataCleanupJob extends AbstractHadoopJob {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false)
            .withDescription("Delete the unused metadata").create("delete");

    @SuppressWarnings("static-access")
    private static final Option OPTION_THRESHOLD_FOR_JOB = OptionBuilder.withArgName("jobThreshold").hasArg()
            .isRequired(false).withDescription("Specify how many days of job metadata keeping. Default 30 days")
            .create("jobThreshold");

    protected static final Logger logger = LoggerFactory.getLogger(MetadataCleanupJob.class);
    private boolean delete = false;
    private KylinConfig config = null;
    private static final long TIME_THREADSHOLD = 1 * 3600 * 1000L; // 1 hour
    private static final int DEFAULT_DAY_THREADSHOLD_FOR_JOB = 30; // 30 days

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        logger.info("jobs args: " + Arrays.toString(args));
        options.addOption(OPTION_DELETE);
        options.addOption(OPTION_THRESHOLD_FOR_JOB);
        parseOptions(options, args);

        logger.info("options: '" + getOptionsAsString() + "'");
        logger.info("delete option value: '" + getOptionValue(OPTION_DELETE) + "'");
        logger.info("jobThreshold option value: '" + getOptionValue(OPTION_THRESHOLD_FOR_JOB) + "'");
        delete = Boolean.parseBoolean(getOptionValue(OPTION_DELETE));
        int jobThresholdDay = optionsHelper.hasOption(OPTION_THRESHOLD_FOR_JOB)
                ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_THRESHOLD_FOR_JOB))
                : DEFAULT_DAY_THREADSHOLD_FOR_JOB;
        cleanup(delete, jobThresholdDay);
        return 0;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(config);
    }

    private boolean isOlderThanThreshold(long resourceTime) {
        long currentTime = System.currentTimeMillis();
        return currentTime - resourceTime > TIME_THREADSHOLD;
    }

    public List<String> cleanup(boolean delete, int jobThresholdDay) throws Exception {
        config = KylinConfig.getInstanceFromEnv();
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<String> toDeleteResource = Lists.newArrayList();

        // two level resources, snapshot tables and cube statistics
        for (String resourceRoot : new String[]{ResourceStore.SNAPSHOT_RESOURCE_ROOT,
                ResourceStore.CUBE_STATISTICS_ROOT}) {
            NavigableSet<String> snapshotTables = getStore().listResources(resourceRoot);

            if (snapshotTables != null) {
                for (String snapshotTable : snapshotTables) {
                    NavigableSet<String> snapshotNames = getStore().listResources(snapshotTable);
                    if (snapshotNames != null)
                        for (String snapshot : snapshotNames) {
                            if (isOlderThanThreshold(getStore().getResourceTimestamp(snapshot)))
                                toDeleteResource.add(snapshot);
                        }
                }
            }
        }

        // three level resources, only dictionaries
        NavigableSet<String> dictTables = getStore().listResources(ResourceStore.DICT_RESOURCE_ROOT);

        if (dictTables != null) {
            for (String table : dictTables) {
                NavigableSet<String> tableColNames = getStore().listResources(table);
                if (tableColNames != null)
                    for (String tableCol : tableColNames) {
                        NavigableSet<String> dictionaries = getStore().listResources(tableCol);
                        if (dictionaries != null)
                            for (String dict : dictionaries)
                                if (isOlderThanThreshold(getStore().getResourceTimestamp(dict)))
                                    toDeleteResource.add(dict);
                    }
            }
        }

        Set<String> activeResourceList = Sets.newHashSet();
        for (org.apache.kylin.cube.CubeInstance cube : cubeManager.listAllCubes()) {
            for (org.apache.kylin.cube.CubeSegment segment : cube.getSegments()) {
                activeResourceList.addAll(segment.getSnapshotPaths());
                activeResourceList.addAll(segment.getDictionaryPaths());
                activeResourceList.add(segment.getStatisticsResourcePath());
            }
        }

        toDeleteResource.removeAll(activeResourceList);

        // delete old and completed jobs
        ExecutableDao executableDao = ExecutableDao.getInstance(KylinConfig.getInstanceFromEnv());
        List<ExecutablePO> allExecutable = executableDao.getJobs();
        for (ExecutablePO executable : allExecutable) {
            long lastModified = executable.getLastModified();
            ExecutableOutputPO output = executableDao.getJobOutput(executable.getUuid());
            long jobThresholdTime = jobThresholdDay * 24 * 3600 * 1000L;

            if (System.currentTimeMillis() - lastModified > jobThresholdTime
                    && (ExecutableState.SUCCEED.toString().equals(output.getStatus())
                    || ExecutableState.DISCARDED.toString().equals(output.getStatus()))) {
                toDeleteResource.add(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + executable.getUuid());
                toDeleteResource.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + executable.getUuid());

                for (ExecutablePO task : executable.getTasks()) {
                    toDeleteResource.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + task.getUuid());
                }
            }
        }

        if (toDeleteResource.size() > 0) {
            logger.info(
                    "The following resources have no reference or is too old, will be cleaned from metadata store: \n");

            for (String s : toDeleteResource) {
                logger.info(s);
                if (delete) {
                    getStore().deleteResource(s);
                }
            }
        } else {
            logger.info("No resource to be cleaned up from metadata store;");
        }
        return toDeleteResource;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MetadataCleanupJob(), args);
        System.exit(exitCode);
    }
}
