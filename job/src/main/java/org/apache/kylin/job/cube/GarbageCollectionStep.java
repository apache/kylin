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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.cmd.ShellCmdOutput;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Drop the resources that is no longer needed, including intermediate hive table (after cube build) and hbase tables (after cube merge)
 */
public class GarbageCollectionStep extends AbstractExecutable {

    private static final String OLD_HTABLES = "oldHTables";

    private static final String OLD_HIVE_TABLE = "oldHiveTable";

    private static final String OLD_HDFS_PATHS = "oldHdfsPaths";

    private static final Logger logger = LoggerFactory.getLogger(GarbageCollectionStep.class);

    private StringBuffer output;

    public GarbageCollectionStep() {
        super();
        output = new StringBuffer();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {

        try {
            dropHBaseTable(context);
            dropHiveTable(context);
            dropHdfsPath(context);
        } catch (IOException e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            output.append("\n").append(e.getLocalizedMessage());
            return new ExecuteResult(ExecuteResult.State.ERROR, output.toString());
        }

        return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
    }

    private void dropHiveTable(ExecutableContext context) throws IOException {
        final String hiveTable = this.getOldHiveTable();
        if (StringUtils.isNotEmpty(hiveTable)) {
            final String dropSQL = "USE " + KylinConfig.getInstanceFromEnv().getHiveDatabaseForIntermediateTable() + ";" + " DROP TABLE IF EXISTS  " + hiveTable + ";";
            final String dropHiveCMD = "hive -e \"" + dropSQL + "\"";
            logger.info("executing: " + dropHiveCMD);
            ShellCmdOutput shellCmdOutput = new ShellCmdOutput();
            context.getConfig().getCliCommandExecutor().execute(dropHiveCMD, shellCmdOutput);
            logger.debug("Dropped Hive table " + hiveTable + " \n");
            output.append(shellCmdOutput.getOutput() + " \n");
            output.append("Dropped Hive table " + hiveTable + " \n");
        }

    }

    private void dropHBaseTable(ExecutableContext context) throws IOException {
        List<String> oldTables = getOldHTables();
        if (oldTables != null && oldTables.size() > 0) {
            String metadataUrlPrefix = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
            Admin admin = null;
            try {
                admin = HBaseConnection.get().getAdmin();
                for (String table : oldTables) {
                    if (admin.tableExists(TableName.valueOf(table))) {
                        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(table));
                        String host = tableDescriptor.getValue(IRealizationConstants.HTableTag);
                        if (metadataUrlPrefix.equalsIgnoreCase(host)) {
                            if (admin.isTableEnabled(TableName.valueOf(table))) {
                                admin.disableTable(TableName.valueOf(table));
                            }
                            admin.deleteTable(TableName.valueOf(table));
                            logger.debug("Dropped HBase table " + table);
                            output.append("Dropped HBase table " + table + " \n");
                        } else {
                            logger.debug("Skipped HBase table " + table);
                            output.append("Skipped HBase table " + table + " \n");
                        }
                    }
                }

            } finally {
                if (admin != null)
                    try {
                        admin.close();
                    } catch (IOException e) {
                        logger.error(e.getLocalizedMessage());
                    }
            }
        }
    }

    private void dropHdfsPathOnCluster(List<String> oldHdfsPaths, FileSystem fileSystem) throws IOException {
        if (oldHdfsPaths != null && oldHdfsPaths.size() > 0) {
            logger.debug("Drop HDFS path on FileSystem: " + fileSystem.getUri());
            output.append("Drop HDFS path on FileSystem: \"" + fileSystem.getUri() + "\" \n");
            for (String path : oldHdfsPaths) {
                if (path.endsWith("*"))
                    path = path.substring(0, path.length() - 1);

                Path oldPath = new Path(path);
                if (fileSystem.exists(oldPath)) {
                    fileSystem.delete(oldPath, true);
                    logger.debug("Dropped HDFS path: " + path);
                    output.append("Dropped HDFS path  \"" + path + "\" \n");
                } else {
                    logger.debug("HDFS path not exists: " + path);
                    output.append("HDFS path not exists: \"" + path + "\" \n");
                }
            }
        }
    }

    private void dropHdfsPath(ExecutableContext context) throws IOException {
        List<String> oldHdfsPaths = this.getOldHdfsPaths();
        FileSystem fileSystem = FileSystem.get(HadoopUtil.getCurrentConfiguration());
        dropHdfsPathOnCluster(oldHdfsPaths, fileSystem);
        
        if (StringUtils.isNotEmpty(context.getConfig().getHBaseClusterFs())) {
            fileSystem = FileSystem.get(HadoopUtil.getCurrentHBaseConfiguration());
            dropHdfsPathOnCluster(oldHdfsPaths, fileSystem);
        }
        
    }

    public void setOldHTables(List<String> tables) {
        setArrayParam(OLD_HTABLES, tables);
    }

    private List<String> getOldHTables() {
        return getArrayParam(OLD_HTABLES);
    }

    public void setOldHdfsPaths(List<String> paths) {
        setArrayParam(OLD_HDFS_PATHS, paths);
    }

    private List<String> getOldHdfsPaths() {
        return getArrayParam(OLD_HDFS_PATHS);
    }

    private void setArrayParam(String paramKey, List<String> paramValues) {
        setParam(paramKey, StringUtils.join(paramValues, ","));
    }

    private List<String> getArrayParam(String paramKey) {
        final String ids = getParam(paramKey);
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
