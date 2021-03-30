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

package org.apache.kylin.storage.hbase.lookup;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.dict.lookup.IExtLookupTableCache.CacheState;
import org.apache.kylin.engine.mr.steps.lookup.LookupExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.ExecuteResult.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateSnapshotCacheForQueryServersStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(UpdateSnapshotCacheForQueryServersStep.class);

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final String tableName = LookupExecutableUtil.getLookupTableName(this.getParams());
        final String snapshotID = LookupExecutableUtil.getLookupSnapshotID(this.getParams());
        final String projectName = LookupExecutableUtil.getProjectName(this.getParams());

        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        int checkInterval = 10 * 1000;
        int maxCheckTime = 10 * 60 * 1000;

        StringWriter outputWriter = new StringWriter();
        PrintWriter pw = new PrintWriter(outputWriter);
        String[] restServers = config.getRestServers();
        List<String> serversNeedCheck = Lists.newArrayList();
        for (String restServer : restServers) {
            logger.info("send build lookup table cache request to server: " + restServer);
            try {
                RestClient restClient = new RestClient(restServer);
                restClient.buildLookupSnapshotCache(projectName, tableName, snapshotID);
                serversNeedCheck.add(restServer);
            } catch (IOException e) {
                logger.error("error when send build cache request to rest server:" + restServer, e);
                pw.println("cache build fail for rest server:" + restServer);
            }
        }
        if (serversNeedCheck.isEmpty()) {
            return new ExecuteResult(State.SUCCEED, outputWriter.toString());
        }

        List<String> completeServers = Lists.newArrayList();
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < maxCheckTime) {
            serversNeedCheck.removeAll(completeServers);
            if (serversNeedCheck.isEmpty()) {
                break;
            }
            for (String restServer : serversNeedCheck) {
                logger.info("check lookup table cache build status for server: " + restServer);
                try {
                    RestClient restClient = new RestClient(restServer);
                    String stateName = restClient.getLookupSnapshotCacheState(tableName, snapshotID);
                    if (!stateName.equals(CacheState.IN_BUILDING.name())) {
                        completeServers.add(restServer);
                        pw.println("cache build complete for rest server:" + restServer + " cache state:" + stateName);
                    }
                } catch (IOException e) {
                    logger.error("error when send build cache request to rest server:" + restServer, e);
                }
            }
            try {
                Thread.sleep(checkInterval);
            } catch (InterruptedException e) {
                logger.error("interrupted", e);
            }
        }
        serversNeedCheck.removeAll(completeServers);
        if (!serversNeedCheck.isEmpty()) {
            pw.println();
            pw.println("check timeout!");
            pw.println("servers not complete:" + serversNeedCheck);
        }
        return new ExecuteResult(State.SUCCEED, outputWriter.toString());
    }

}
