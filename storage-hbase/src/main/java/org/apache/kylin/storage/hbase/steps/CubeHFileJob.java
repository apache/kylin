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

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author George Song (ysong1)
 */
public class CubeHFileJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(CubeHFileJob.class);

    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_HTABLE_NAME);
            parseOptions(options, args);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            if(KylinConfig.getInstanceFromEnv().isTransformPathToMasterNN()) {
                output = HadoopUtil.transformPathToNN(output);
            }
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

            CubeInstance cube = cubeMgr.getCube(cubeName);
            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));

            setJobClasspath(job);

            addInputDirs(getOptionValue(OPTION_INPUT_PATH), job);
            FileOutputFormat.setOutputPath(job, output);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(CubeHFileMapper.class);
            job.setReducerClass(KeyValueSortReducer.class);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            Configuration conf = HBaseConfiguration.create(getConf());
            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            String tableName = getOptionValue(OPTION_HTABLE_NAME).toUpperCase();
            HTable htable = new HTable(conf, tableName);

            //Automatic config !
            HFileOutputFormat.configureIncrementalLoad(job, htable);

            // set block replication to 3 for hfiles
            conf.set(DFSConfigKeys.DFS_REPLICATION_KEY, "3");

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in CubeHFileJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CubeHFileJob(), args);
        System.exit(exitCode);
    }

}
