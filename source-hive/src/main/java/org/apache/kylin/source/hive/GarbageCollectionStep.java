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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class GarbageCollectionStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(GarbageCollectionStep.class);

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = context.getConfig();
        StringBuffer output = new StringBuffer();
        try {
            output.append(cleanUpIntermediateFlatTable(config));
        } catch (IOException e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return ExecuteResult.createError(e);
        }

        return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
    }

    //clean up both hive intermediate flat table and view table
    private String cleanUpIntermediateFlatTable(KylinConfig config) throws IOException {
        StringBuffer output = new StringBuffer();
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(getName());
        final List<String> hiveTables = this.getIntermediateTables();
        if (!config.isHiveKeepFlatTable()) {
            for (String hiveTable : hiveTables) {
                if (StringUtils.isNotEmpty(hiveTable)) {
                    hiveCmdBuilder.addStatement("USE " + config.getHiveDatabaseForIntermediateTable() + ";");
                    hiveCmdBuilder
                            .addStatement("DROP TABLE IF EXISTS " + hiveTable + ";");

                    output.append("Hive table " + hiveTable + " is dropped. \n");
                }
            }
            rmdirOnHDFS(getExternalDataPaths());
        }
        config.getCliCommandExecutor().execute(hiveCmdBuilder.build());
        output.append("Path " + getExternalDataPaths() + " is deleted. \n");

        return output.toString();
    }

    private void rmdirOnHDFS(List<String> paths) throws IOException {
        for (String path : paths) {
            Path externalDataPath = new Path(path);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(externalDataPath)) {
                fs.delete(externalDataPath, true);
            }
        }
    }

    public void setIntermediateTables(List<String> tableIdentity) {
        setParam("oldHiveTables", StringUtil.join(tableIdentity, ","));
    }

    //get intermediate fact table and lookup table(if exists)
    private List<String> getIntermediateTables() {
        List<String> intermediateTables = Lists.newArrayList();
        String hiveTables = getParam("oldHiveTables");
        if (this.getParams().containsKey("oldHiveViewIntermediateTables")) {
            hiveTables += ("," + getParam("oldHiveViewIntermediateTables"));
        }
        String[] tables = StringUtil.splitAndTrim(hiveTables, ",");
        for (String t : tables) {
            intermediateTables.add(t);
        }
        return intermediateTables;
    }

    public void setExternalDataPaths(List<String> externalDataPaths) {
        setParam("externalDataPaths", StringUtil.join(externalDataPaths, ","));
    }

    private List<String> getExternalDataPaths() {
        String[] paths = StringUtil.splitAndTrim(getParam("externalDataPaths"), ",");
        List<String> result = Lists.newArrayList();
        for (String s : paths) {
            result.add(s);
        }
        return result;
    }

    public void setHiveViewIntermediateTableIdentities(String tableIdentities) {
        setParam("oldHiveViewIntermediateTables", tableIdentities);
    }
}