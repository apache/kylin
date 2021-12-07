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

import org.apache.kylin.job.impl.threadpool.IJobRunner;
import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.collect.ImmutableList;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.IEngineAware;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class CreateMrHiveDictStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CreateMrHiveDictStep.class);
    private final PatternedLogger stepLogger = new PatternedLogger(logger);
    private final Lock threadLock = new ReentrantLock();
    private static final String GET_SQL = "\" Get Max Dict Value Sql : \"";

    protected void createMrHiveDict(KylinConfig config, DistributedLock lock) throws Exception {
        logger.info("Start to run createMrHiveDict {}", getId());
        try {
            // Step 1: Apply for lock if required
            if (getIsLock()) {
                MRHiveDictUtil.getLock(getCubeName(), getId(), lock, threadLock, stepLogger);
            }

            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(getName());
            hiveCmdBuilder.overwriteHiveProps(config.getHiveConfigOverride());
            hiveCmdBuilder.addStatement(getInitStatement());

            String sql = getCreateTableStatement();
            if (sql != null && sql.length() > 0) {
                hiveCmdBuilder.addStatement(sql);
            }
            Map<String, String> maxDictValMap = deserializeForMap(getMaxDictStatementMap());
            Map<String, String> dictSqlMap = deserializeForMap(getCreateTableStatementMap());

            // Step 2: Execute HQL
            if (!dictSqlMap.isEmpty()) {
                IHiveClient hiveClient = HiveClientFactory.getHiveClient();
                if (!maxDictValMap.isEmpty()) {
                    if (maxDictValMap.size() == dictSqlMap.size()) {
                        maxDictValMap.forEach((columnName, maxDictValSql) -> {
                            int max = 0;
                            List<Object[]> datas = null;
                            try {
                                datas = hiveClient.getHiveResult(maxDictValSql);
                                if (Objects.nonNull(datas) && !datas.isEmpty()) {
                                    max = Integer.valueOf(datas.get(0)[0] + "");
                                    stepLogger.log(columnName + GET_SQL + maxDictValSql);
                                    stepLogger.log(columnName + " Get Max Dict Value Of : " + max);
                                } else {
                                    stepLogger.log(columnName + GET_SQL + maxDictValSql);
                                    stepLogger.log(columnName + " Get Max Dict Value Of ERROR: hive execute result is null.");
                                    throw new IOException("execute get max dict result fail : " + maxDictValSql);
                                }
                            } catch (Exception e) {
                                stepLogger.log(columnName + GET_SQL + maxDictValSql);
                                stepLogger.log(columnName + " Get Max Dict Value Of ERROR :" + e.getMessage());
                                logger.error("execute get max dict result fail : " + maxDictValSql, e);
                            }
                            String dictSql = dictSqlMap.get(columnName).replace("___maxDictVal___", max + "");
                            hiveCmdBuilder.addStatement(dictSql);
                        });
                    } else {
                        logger.error("Max Dict Value size is not equals Dict Sql size ! ");
                    }
                } else {
                    dictSqlMap.forEach((columnName, dictSql) -> hiveCmdBuilder.addStatement(dictSql));
                }
            }

            final String cmd = hiveCmdBuilder.toString();

            stepLogger.log("Build Hive Global Dictionary by: " + cmd);

            CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = manager.getCube(getCubeName());

            if (config.isLivyEnabled() && cube.getEngineType() == IEngineAware.ID_SPARK) {
                MRHiveDictUtil.runLivySqlJob(stepLogger, config, ImmutableList.copyOf(hiveCmdBuilder.getStatements()), getManager(), getId());
            } else {
                Pair<Integer, String> response = config.getCliCommandExecutor().execute(cmd, stepLogger);
                if (response.getFirst() != 0) {
                    throw new RuntimeException("Failed to create MR/Hive dict, error code " + response.getFirst());
                }
            }

            // Step 3: Release lock if required
            if (getIsUnlock()) {
                MRHiveDictUtil.unLock(getCubeName(), getId(), lock, stepLogger);
            }
            getManager().addJobInfo(getId(), stepLogger.getInfo());
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        }
    }

    @Override
    public KylinConfig getCubeSpecificConfig() {
        String cubeName = CubingExecutableUtil.getCubeName(getParams());
        CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = manager.getCube(cubeName);
        return cube.getConfig();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context, IJobRunner jobRunner) throws ExecuteException {
        KylinConfig config = getCubeSpecificConfig();
        DistributedLock lock = null;
        try {
            if (getIsLock() || getIsUnlock()) {
                lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();
            }

            createMrHiveDict(config, lock);

            if (isDiscarded()) {
                if (getIsLock() && lock != null) {
                    MRHiveDictUtil.unLock(getCubeName(), getId(), lock, stepLogger);
                }
                return new ExecuteResult(ExecuteResult.State.DISCARDED, stepLogger.getBufferedLog());
            } else {
                return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());
            }
        } catch (Exception e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            if (isDiscarded()) {
                if (getIsLock()) {
                    MRHiveDictUtil.unLock(getCubeName(), getId(), lock, stepLogger);
                }
                return new ExecuteResult(ExecuteResult.State.DISCARDED, stepLogger.getBufferedLog());
            } else {
                return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
            }
        }
    }

    private void doRetry(String cmd, KylinConfig config) throws Exception {
        if (Objects.nonNull(cmd)) {
            stepLogger.log("cmd : " + cmd);
            int currTimes = 0;
            int maxTimes = 360;
            boolean flag = true;
            while (flag && currTimes <= maxTimes) {
                try {
                    Pair<Integer, String> result = config.getCliCommandExecutor().execute(cmd, stepLogger);
                    stepLogger.log(result.toString());
                    flag = false;
                } catch (Exception e) {
                    stepLogger.log("execute : " + cmd + " Failed && And errLog is " + e.getMessage());
                    Thread.sleep(60000);
                    currTimes += 60;
                }
            }
        }
    }


    public void setInitStatement(String sql) {
        setParam("HiveInit", sql);
    }

    public String getInitStatement() {
        return getParam("HiveInit");
    }

    public void setCreateTableStatement(String sql) {
        setParam("HiveRedistributeData", sql);
    }

    public String getCreateTableStatement() {
        return getParam("HiveRedistributeData");
    }

    public void setCreateTableStatementMap(Map<String, String> dictSqlMap) {
        setParam("DictSqlMap", serializeMap(dictSqlMap));
    }

    public String getCreateTableStatementMap() {
        return getParam("DictSqlMap");
    }

    public void setMaxDictStatementMap(Map<String, String> maxDictValMap) {
        setParam("DictMaxMap", serializeMap(maxDictValMap));
    }

    public String getMaxDictStatementMap() {
        return getParam("DictMaxMap");
    }

    public void setIsLock(Boolean isLock) {
        setParam("isLock", String.valueOf(isLock));
    }

    public boolean getIsLock() {
        String isLock = getParam("isLock");
        return !Strings.isNullOrEmpty(isLock) && Boolean.parseBoolean(isLock);
    }

    public void setJobFlowJobId(String jobId) {
        setParam("jobFlowJobId", jobId);
    }

    public String getJobFlowJobId() {
        return getParam("jobFlowJobId");
    }

    public void setIsUnLock(Boolean isUnLock) {
        setParam("isUnLock", String.valueOf(isUnLock));
    }

    public boolean getIsUnlock() {
        String isUnLock = getParam("isUnLock");
        return !Strings.isNullOrEmpty(isUnLock) && Boolean.parseBoolean(isUnLock);
    }

    public void setLockPathName(String pathName) {
        setParam("lockPathName", pathName);
    }


    private static String serializeMap(Map<String, String> map) {
        JSONArray result = new JSONArray();
        if (map != null && map.size() > 0) {
            map.forEach((key, value) -> {
                JSONObject jsonObject = new JSONObject();
                try {
                    jsonObject.put(key, value);
                } catch (JSONException e) {
                    logger.error("Json Error", e);
                }
                result.put(jsonObject);
            });
        }
        return result.toString();
    }

    private static Map<String, String> deserializeForMap(String mapStr) {
        Map<String, String> result = new HashMap<>();
        if (mapStr != null) {
            try {
                JSONArray jsonArray = new JSONArray(mapStr);
                int size = jsonArray.length();
                for (int i = 0; i < size; i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    Iterator<String> iterator = jsonObject.keys();
                    while (iterator.hasNext()) {
                        String key = iterator.next();
                        String value = jsonObject.getString(key);
                        result.put(key, value);
                    }
                }
            } catch (JSONException e) {
                logger.error("Json Error", e);
            }
        }
        return result;
    }

}
