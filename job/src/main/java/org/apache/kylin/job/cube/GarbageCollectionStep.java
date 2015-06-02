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

package org.apache.kylin.job.cube;

import com.google.common.collect.Lists;
import jodd.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.job.cmd.ShellCmdOutput;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Drop the resources that is no longer needed, including intermediate hive table (after cube build) and hbase tables (after cube merge)
 */
public class GarbageCollectionStep extends AbstractExecutable {

    private static final String OLD_HTABLES = "oldHTables";

    private static final String OLD_HIVE_TABLE = "oldHiveTable";

    private static final Logger logger = LoggerFactory.getLogger(GarbageCollectionStep.class);

    public GarbageCollectionStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {

        ExecuteResult.State state = null;
        StringBuffer output = new StringBuffer();

        final String hiveTable = this.getOldHiveTable();
        if (StringUtil.isNotEmpty(hiveTable)) {
            final String dropHiveCMD = "hive -e \"DROP TABLE IF EXISTS  " + hiveTable + ";\"";
            ShellCmdOutput shellCmdOutput = new ShellCmdOutput();
            try {
                context.getConfig().getCliCommandExecutor().execute(dropHiveCMD, shellCmdOutput);
                output.append("Hive table " + hiveTable + " is dropped. \n");
            } catch (IOException e) {
                logger.error("job:" + getId() + " execute finished with exception", e);
                output.append(shellCmdOutput.getOutput()).append("\n").append(e.getLocalizedMessage());
                return new ExecuteResult(ExecuteResult.State.ERROR, output.toString());
            }
        }


        List<String> oldTables = getOldHTables();
        if (oldTables != null && oldTables.size() > 0) {
            Configuration conf = HBaseConfiguration.create();
            HBaseAdmin admin = null;
            try {
                admin = new HBaseAdmin(conf);
                for (String table : oldTables) {
                    if (admin.tableExists(table)) {
                        if (admin.isTableEnabled(table)) {
                            admin.disableTable(table);
                        }

                        admin.deleteTable(table);
                    }
                    logger.debug("Dropped htable: " + table);
                    output.append("HBase table " + table + " is dropped. \n");
                }

            } catch (IOException e) {
                output.append("Got error when drop HBase table, exiting... \n");
                // This should not block the merge job; Orphans should be cleaned up in StorageCleanupJob
                return new ExecuteResult(ExecuteResult.State.ERROR, output.append(e.getLocalizedMessage()).toString());
            } finally {
                if (admin != null)
                    try {
                        admin.close();
                    } catch (IOException e) {
                        logger.error(e.getLocalizedMessage());
                    }
            }
        }


        return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
    }

    public void setOldHTables(List<String> ids) {
        setParam(OLD_HTABLES, StringUtils.join(ids, ","));
    }

    private List<String> getOldHTables() {
        final String ids = getParam(OLD_HTABLES);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            for (String id : splitted) {
                result.add(id);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public void setOldHiveTable(String hiveTable) {
        setParam(OLD_HIVE_TABLE, hiveTable);
    }

    private String getOldHiveTable() {
        return getParam(OLD_HIVE_TABLE);
    }

}
