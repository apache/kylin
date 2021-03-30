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

package org.apache.kylin.storage.hbase.steps;

import com.google.common.collect.Lists;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author fengpod
 */
public class HFileDistcpJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(HFileDistcpJob.class);

    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_HTABLE_NAME);
            parseOptions(options, args);

            // use current hbase configuration
            Configuration configuration = new Configuration();
            HBaseConnection.addHBaseClusterNNHAConfiguration(configuration);

            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            FileSystem fs = HadoopUtil.getFileSystem(output, configuration);
            if (fs.exists(output) == false) {
                fs.mkdirs(output);
            }

            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);

            List<Path> sourceList = Lists.newArrayList();
            sourceList.add(input);
            DistCpOptions distCpOptions = new DistCpOptions(sourceList, output);
            distCpOptions.setMapBandwidth(cube.getConfig().getDistCPMapBandWidth());
            distCpOptions.setMaxMaps(cube.getConfig().getDistCPMaxMapNum());
            distCpOptions.setOverwrite(true);
            distCpOptions.setBlocking(true);

            configuration.set("mapreduce.job.name", getOptionValue(OPTION_JOB_NAME));
            DistCp distCp = new DistCp(configuration, distCpOptions);

            job = distCp.execute();

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            // add metadata to distributed cache
            attachCubeMetadata(cube, job.getConfiguration());
        } catch (Exception e){
            throw e;
        }finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HFileDistcpJob(), args);
        System.exit(exitCode);
    }

}