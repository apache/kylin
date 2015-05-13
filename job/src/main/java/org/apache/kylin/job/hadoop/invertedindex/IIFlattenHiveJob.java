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
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.cmd.ICommandOutput;
import org.apache.kylin.job.cmd.ShellCmd;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.invertedindex.model.IIJoinedFlatTableDesc;

/**
 */
public class IIFlattenHiveJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(IIFlattenHiveJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        try {
            options.addOption(OPTION_II_NAME);
            parseOptions(options, args);

            String iiname = getOptionValue(OPTION_II_NAME);
            KylinConfig config = KylinConfig.getInstanceFromEnv();

            IIInstance iiInstance = IIManager.getInstance(config).getII(iiname);
            IIDesc iidesc = IIDescManager.getInstance(config).getIIDesc(iiInstance.getDescName());

            String jobUUID = "00bf87b5-c7b5-4420-a12a-07f6b37b3187";
            JobEngineConfig engineConfig = new JobEngineConfig(config);
            IJoinedFlatTableDesc intermediateTableDesc = new IIJoinedFlatTableDesc(iidesc);
            String dropTableHql = JoinedFlatTable.generateDropTableStatement(intermediateTableDesc, jobUUID);
            String createTableHql = JoinedFlatTable.generateCreateTableStatement(intermediateTableDesc, //
                    JobInstance.getJobWorkingDir(jobUUID, engineConfig.getHdfsWorkingDirectory()), jobUUID);
            String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(intermediateTableDesc, jobUUID, engineConfig);

            StringBuffer buf = new StringBuffer();
            buf.append("hive -e \"");
            buf.append(dropTableHql + "\n");
            buf.append(createTableHql + "\n");
            buf.append(insertDataHqls + "\n");
            buf.append("\"");
            
            System.out.println(buf.toString());
            System.out.println("========================");

            ShellCmd cmd = new ShellCmd(buf.toString(), null, 0, null, null, false);
            ICommandOutput output = cmd.execute();
            System.out.println(output.getOutput());
            System.out.println(output.getExitCode());
            
            return 0;
        } catch (Exception e) {
            logger.error("error execute IIFlattenHiveJob", e);
            printUsage(options);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        IIFlattenHiveJob job = new IIFlattenHiveJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
