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

package org.apache.kylin.source.hive;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableList;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;


public class RedistributeFlatHiveTableByLivyStep extends AbstractExecutable {
    private final PatternedLogger stepLogger = new PatternedLogger(logger);

    private long computeRowCount(String database, String table) throws Exception {
        IHiveClient hiveClient = HiveClientFactory.getHiveClient();
        return hiveClient.getHiveTableRows(database, table);
    }

    private long getDataSize(String database, String table) throws Exception {
        IHiveClient hiveClient = HiveClientFactory.getHiveClient();
        long size = hiveClient.getHiveTableMeta(database, table).fileSize;
        return size;
    }

    private void redistributeTable(KylinConfig config, int numReducers) throws Exception {
        StringBuffer statement = new StringBuffer();
        statement.append(getInitStatement());
        statement.append("set mapreduce.job.reduces=" + numReducers + ";\n");
        statement.append("set hive.merge.mapredfiles=false;\n");
        statement.append(getRedistributeDataStatement());

        stepLogger.log("Redistribute table, cmd: ");
        stepLogger.log(statement.toString());

        MRHiveDictUtil.runLivySqlJob(stepLogger, config, ImmutableList.of(statement.toString()), getManager(), getId());
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = getCubeSpecificConfig();
        String intermediateTable = getIntermediateTable();
        String database, tableName;
        if (intermediateTable.indexOf(".") > 0) {
            database = intermediateTable.substring(0, intermediateTable.indexOf("."));
            tableName = intermediateTable.substring(intermediateTable.indexOf(".") + 1);
        } else {
            database = config.getHiveDatabaseForIntermediateTable();
            tableName = intermediateTable;
        }

        try {
            long rowCount = computeRowCount(database, tableName);
            logger.debug("Row count of table '" + intermediateTable + "' is " + rowCount);
            if (rowCount == 0) {
                if (!config.isEmptySegmentAllowed()) {
                    stepLogger.log("Detect upstream hive table is empty, "
                            + "fail the job because \"kylin.job.allow-empty-segment\" = \"false\"");
                    return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
                } else {
                    return new ExecuteResult(ExecuteResult.State.SUCCEED, "Row count is 0, no need to redistribute");
                }
            }

            int mapperInputRows = config.getHadoopJobMapperInputRows();

            int numReducers = Math.round(rowCount / ((float) mapperInputRows));
            numReducers = Math.max(1, numReducers);
            numReducers = Math.min(numReducers, config.getHadoopJobMaxReducerNumber());

            stepLogger.log("total input rows = " + rowCount);
            stepLogger.log("expected input rows per mapper = " + mapperInputRows);
            stepLogger.log("num reducers for RedistributeFlatHiveTableStep = " + numReducers);

            redistributeTable(config, numReducers);
            long dataSize = getDataSize(database, tableName);
            getManager().addJobInfo(getId(), ExecutableConstants.HDFS_BYTES_WRITTEN, "" + dataSize);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog(), e);
        }
    }

    public void setInitStatement(String sql) {
        setParam("HiveInit", sql);
    }

    public String getInitStatement() {
        return getParam("HiveInit");
    }

    public void setRedistributeDataStatement(String sql) {
        setParam("HiveRedistributeData", sql);
    }

    public String getRedistributeDataStatement() {
        return getParam("HiveRedistributeData");
    }

    public String getIntermediateTable() {
        return getParam("intermediateTable");
    }

    public void setIntermediateTable(String intermediateTable) {
        setParam("intermediateTable", intermediateTable);
    }
}