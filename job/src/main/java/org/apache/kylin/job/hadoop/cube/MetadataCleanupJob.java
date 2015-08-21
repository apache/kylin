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

package org.apache.kylin.job.hadoop.cube;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MetadataCleanupJob extends AbstractHadoopJob {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false).withDescription("Delete the unused metadata").create("delete");

    protected static final Logger log = LoggerFactory.getLogger(MetadataCleanupJob.class);

    boolean delete = false;

    private KylinConfig config = null;

    public static final long TIME_THREADSHOLD = 2 * 24 * 3600 * 1000l; // 2 days

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        log.info("----- jobs args: " + Arrays.toString(args));
        try {
            options.addOption(OPTION_DELETE);
            parseOptions(options, args);

            log.info("options: '" + getOptionsAsString() + "'");
            log.info("delete option value: '" + getOptionValue(OPTION_DELETE) + "'");
            delete = Boolean.parseBoolean(getOptionValue(OPTION_DELETE));

            config = KylinConfig.getInstanceFromEnv();

            cleanup();

            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            throw e;
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(config);
    }

    private boolean isOlderThanThreshold(long resourceTime) {
        long currentTime = System.currentTimeMillis();

        if (currentTime - resourceTime > TIME_THREADSHOLD)
            return true;
        return false;
    }

    public void cleanup() throws Exception {
        CubeManager cubeManager = CubeManager.getInstance(config);

        Set<String> activeResourceList = Sets.newHashSet();
        for (org.apache.kylin.cube.CubeInstance cube : cubeManager.listAllCubes()) {
            for (org.apache.kylin.cube.CubeSegment segment : cube.getSegments()) {
                activeResourceList.addAll(segment.getSnapshotPaths());
                activeResourceList.addAll(segment.getDictionaryPaths());
            }
        }

        List<String> toDeleteResource = Lists.newArrayList();

        // two level resources, snapshot tables and cube statistics
        for (String resourceRoot : new String[] { ResourceStore.SNAPSHOT_RESOURCE_ROOT }) {
            ArrayList<String> snapshotTables = getStore().listResources(resourceRoot);

            if (snapshotTables != null) {
                for (String snapshotTable : snapshotTables) {
                    ArrayList<String> snapshotNames = getStore().listResources(snapshotTable);
                    if (snapshotNames != null)
                        for (String snapshot : snapshotNames) {
                            if (!activeResourceList.contains(snapshot)) {
                                if (isOlderThanThreshold(getStore().getResourceTimestamp(snapshot)))
                                    toDeleteResource.add(snapshot);
                            }
                        }
                }
            }
        }

        // three level resources, only dictionaries
        ArrayList<String> dictTables = getStore().listResources(ResourceStore.DICT_RESOURCE_ROOT);

        if (dictTables != null) {
            for (String table : dictTables) {
                ArrayList<String> tableColNames = getStore().listResources(table);
                if (tableColNames != null)
                    for (String tableCol : tableColNames) {
                        ArrayList<String> dictionaries = getStore().listResources(tableCol);
                        if (dictionaries != null)
                            for (String dict : dictionaries)
                                if (!activeResourceList.contains(dict)) {
                                    if (isOlderThanThreshold(getStore().getResourceTimestamp(dict)))
                                        toDeleteResource.add(dict);
                                }
                    }
            }
        }

        if (toDeleteResource.size() > 0) {
            logger.info("The following resources have no reference, will be cleaned from metadata store: \n");

            for (String s : toDeleteResource) {
                logger.info(s);
                if (delete == true) {
                    getStore().deleteResource(s);
                }
            }
        } else {
            logger.info("No resource to be cleaned up from metadata store;");
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MetadataCleanupJob(), args);
        System.exit(exitCode);
    }
}
