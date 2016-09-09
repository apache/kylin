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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class CreateFlatHiveTableStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CreateFlatHiveTableStep.class);
    private final BufferedLogger stepLogger = new BufferedLogger(logger);

    private long readRowCountFromFile() throws IOException {
        Path rowCountFile = new Path(getRowCountOutputDir(), "000000_0");

        FileSystem fs = FileSystem.get(rowCountFile.toUri(), HadoopUtil.getCurrentConfiguration());
        InputStream in = fs.open(rowCountFile);
        try {
            String content = IOUtils.toString(in, Charset.defaultCharset());
            return Long.valueOf(content.trim()); // strip the '\n' character

        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    private int determineNumReducer(KylinConfig config, long rowCount) throws IOException {
        int mapperInputRows = config.getHadoopJobMapperInputRows();

        int numReducers = Math.round(rowCount / ((float) mapperInputRows));
        numReducers = Math.max(numReducers, config.getHadoopJobMinReducerNumber());
        numReducers = Math.min(numReducers, config.getHadoopJobMaxReducerNumber());

        stepLogger.log("total input rows = " + rowCount);
        stepLogger.log("expected input rows per mapper = " + mapperInputRows);
        stepLogger.log("reducers for RedistributeFlatHiveTableStep = " + numReducers);

        return numReducers;
    }

    private void createFlatHiveTable(KylinConfig config, int numReducers) throws IOException {
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(getInitStatement());
        boolean useRedistribute = getUseRedistribute();
        if (useRedistribute == true) {
            hiveCmdBuilder.addStatement("set mapreduce.job.reduces=" + numReducers + ";\n");
            hiveCmdBuilder.addStatement("set hive.merge.mapredfiles=false;\n"); //disable merge
        }
        hiveCmdBuilder.addStatement(getCreateTableStatement());
        final String cmd = hiveCmdBuilder.toString();

        stepLogger.log("Create and distribute table, cmd: ");
        stepLogger.log(cmd);

        Pair<Integer, String> response = config.getCliCommandExecutor().execute(cmd, stepLogger);
        if (response.getFirst() != 0) {
            throw new RuntimeException("Failed to create flat hive table, error code " + response.getFirst());
        }
    }

    private KylinConfig getCubeSpecificConfig() {
        String cubeName = CubingExecutableUtil.getCubeName(getParams());
        CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = manager.getCube(cubeName);
        return cube.getConfig();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = getCubeSpecificConfig();
        try {

            boolean useRedistribute = getUseRedistribute();

            int numReducers = 0;
            if (useRedistribute == true) {
                long rowCount = readRowCountFromFile();
                if (!config.isEmptySegmentAllowed() && rowCount == 0) {
                    stepLogger.log("Detect upstream hive table is empty, " + "fail the job because \"kylin.job.allow.empty.segment\" = \"false\"");
                    return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
                }

                numReducers = determineNumReducer(config, rowCount);
            }

            createFlatHiveTable(config, numReducers);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
        }
    }

    public void setInitStatement(String sql) {
        setParam("HiveInit", sql);
    }

    public String getInitStatement() {
        return getParam("HiveInit");
    }

    public void setUseRedistribute(boolean useRedistribute) {
        setParam("useRedistribute", String.valueOf(useRedistribute));
    }

    public boolean getUseRedistribute() {
        return Boolean.valueOf(getParam("useRedistribute"));
    }

    public void setCreateTableStatement(String sql) {
        setParam("HiveRedistributeData", sql);
    }

    public String getCreateTableStatement() {
        return getParam("HiveRedistributeData");
    }

    public void setRowCountOutputDir(String rowCountOutputDir) {
        setParam("rowCountOutputDir", rowCountOutputDir);
    }

    public String getRowCountOutputDir() {
        return getParam("rowCountOutputDir");
    }
}
