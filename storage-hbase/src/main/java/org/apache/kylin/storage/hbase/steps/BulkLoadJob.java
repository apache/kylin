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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ysong1
 * 
 */
public class BulkLoadJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(BulkLoadJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_HTABLE_NAME);
            options.addOption(OPTION_CUBE_NAME);
            parseOptions(options, args);

            String tableName = getOptionValue(OPTION_HTABLE_NAME).toUpperCase();
            // e.g
            // /tmp/kylin-3f150b00-3332-41ca-9d3d-652f67f044d7/test_kylin_cube_with_slr_ready_2_segments/hfile/
            // end with "/"
            String input = getOptionValue(OPTION_INPUT_PATH);

            Configuration conf = HBaseConfiguration.create(getConf());
            FileSystem fs = FileSystem.get(conf);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeDesc cubeDesc = cube.getDescriptor();
            FsPermission permission = new FsPermission((short) 0777);
            for (HBaseColumnFamilyDesc cf : cubeDesc.getHBaseMapping().getColumnFamily()) {
                String cfName = cf.getName();
                Path columnFamilyPath = new Path(input, cfName);

                // File may have already been auto-loaded (in the case of MapR DB)
                if (fs.exists(columnFamilyPath)) {
                    fs.setPermission(columnFamilyPath, permission);
                }
            }

            String[] newArgs = new String[2];
            newArgs[0] = input;
            newArgs[1] = tableName;

            logger.debug("Start to run LoadIncrementalHFiles");
            int ret = ToolRunner.run(new LoadIncrementalHFiles(conf), newArgs);
            logger.debug("End to run LoadIncrementalHFiles");
            return ret;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BulkLoadJob(), args);
        System.exit(exitCode);
    }
}
