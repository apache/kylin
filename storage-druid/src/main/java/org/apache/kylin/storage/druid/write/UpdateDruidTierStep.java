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

package org.apache.kylin.storage.druid.write;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.storage.druid.DruidSchema;
import org.apache.kylin.storage.druid.common.DruidCoordinatorClient;
import org.apache.kylin.storage.druid.common.DruidSerdeHelper;
import org.apache.kylin.storage.druid.common.ForeverLoadRule;
import org.apache.kylin.storage.druid.common.Rule;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.druid.java.util.common.StringUtils;

public class UpdateDruidTierStep extends AbstractExecutable {
    private final BufferedLogger stepLogger = new BufferedLogger(logger);

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final Map<String, String> params = getParams();
        final String cubeName = CubingExecutableUtil.getCubeName(params);
        final DruidCoordinatorClient coordinatorClient = DruidCoordinatorClient.getSingleton();

        try {
            CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = manager.getCube(cubeName);
            final String dataSource = DruidSchema.getDataSource(cube.getDescriptor());

            List<Rule> newRules = getRulesForCube(cube);
            List<Rule> oldRules = coordinatorClient.getRules(dataSource);

            if (newRules.equals(oldRules)) {
                stepLogger.log("Current rules for datasource " + dataSource + " is up to date, done");
            } else {
                stepLogger.log(StringUtils.format("Setting new rules for datasource %s : %s", dataSource,
                        DruidSerdeHelper.JSON_MAPPER.writeValueAsString(newRules)));
                coordinatorClient.putRules(dataSource, newRules);
                stepLogger.log("Done");
            }

            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("UpdateDruidTierStep failed", e);
            stepLogger.log("FAILED! " + e.getMessage());
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
        }
    }

    private List<Rule> getRulesForCube(CubeInstance cube) {
        // TODO consider cube retention
        Map<String, Integer> tieredReplicants = Maps.newHashMap();
        tieredReplicants.put(cube.getConfig().getDruidTierName(), cube.getConfig().getDruidReplicationNum());
        return Lists.<Rule> newArrayList(new ForeverLoadRule(tieredReplicants));
    }

    public void setCubeName(String cubeName) {
        CubingExecutableUtil.setCubeName(cubeName, getParams());
    }
}
