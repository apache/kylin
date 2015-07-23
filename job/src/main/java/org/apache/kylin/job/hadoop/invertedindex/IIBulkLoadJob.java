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

package org.apache.kylin.job.hadoop.invertedindex;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.storage.hbase.HBaseConnection;

/**
 * @author ysong1
 * 
 */
public class IIBulkLoadJob extends AbstractHadoopJob {

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_HTABLE_NAME);
            options.addOption(OPTION_II_NAME);
            parseOptions(options, args);

            String tableName = getOptionValue(OPTION_HTABLE_NAME);
            String input = getOptionValue(OPTION_INPUT_PATH);
            String iiname = getOptionValue(OPTION_II_NAME);

            FileSystem fs = FileSystem.get(getConf());
            FsPermission permission = new FsPermission((short) 0777);
            fs.setPermission(new Path(input, IIDesc.HBASE_FAMILY), permission);

            int hbaseExitCode = ToolRunner.run(new LoadIncrementalHFiles(getConf()), new String[] { input, tableName });

            IIManager mgr = IIManager.getInstance(KylinConfig.getInstanceFromEnv());
            IIInstance ii = mgr.getII(iiname);
            IISegment seg = ii.getFirstSegment();
            seg.setStorageLocationIdentifier(tableName);
            seg.setStatus(SegmentStatusEnum.READY);
            mgr.updateII(ii);

            return hbaseExitCode;

        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        IIBulkLoadJob job = new IIBulkLoadJob();
        job.setConf(HBaseConnection.newHBaseConfiguration(KylinConfig.getInstanceFromEnv().getStorageUrl()));
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
