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

package org.apache.kylin.common.livy;

import org.apache.commons.compress.utils.Lists;
import org.apache.kylin.common.util.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 */
public class LivyRestExecutor {

    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(LivyRestExecutor.class);

    public void execute(LivyRestBuilder livyRestBuilder, Logger logAppender) {
        try {
            long startTime = System.currentTimeMillis();

            String dataJson = livyRestBuilder.build();

            logAppender.log("Livy submit Json: ");
            logAppender.log(dataJson + "\n");

            LivyRestClient restClient = new LivyRestClient();
            String result = restClient.livySubmitJobBatches(dataJson);

            JSONObject jsonObject = new JSONObject(result);
            String state = jsonObject.getString("state");
            logAppender.log("Livy submit Result: " + state);
            logger.info("Livy submit Result: {}", state);

            livyLog(jsonObject, logAppender);

            final String livyTaskId = jsonObject.getString("id");
            while (!LivyStateEnum.shutting_down.toString().equalsIgnoreCase(state)
                    && !LivyStateEnum.error.toString().equalsIgnoreCase(state)
                    && !LivyStateEnum.dead.toString().equalsIgnoreCase(state)
                    && !LivyStateEnum.success.toString().equalsIgnoreCase(state)) {

                String statusResult = restClient.livyGetJobStatusBatches(livyTaskId);
                jsonObject = new JSONObject(statusResult);
                if (!state.equalsIgnoreCase(jsonObject.getString("state"))) {
                    logAppender.log("Livy status Result: " + jsonObject.getString("state"));
                    livyLog(jsonObject, logAppender);
                }

                state = jsonObject.getString("state");
                Thread.sleep(10*1000L);
            }
            if (!LivyStateEnum.success.toString().equalsIgnoreCase(state)) {
                // livy batch failed, get detail log
                String statusResult = restClient.livyGetJobStatusBatches(livyTaskId);
                jsonObject = new JSONObject(statusResult);
                String detailErrorLog = String.join("\n", getLogs(jsonObject));

                logAppender.log("livy start execute failed. state is " + state + ". log is " + detailErrorLog);
                logger.info("livy start execute failed. state is {}", state + ". log is " + detailErrorLog);
                throw new RuntimeException("livy get status failed. state is " + state + ". log is " + detailErrorLog);
            }
            logAppender.log("costTime : " + (System.currentTimeMillis() - startTime) / 1000 + " s");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("livy execute failed. \n" + e.getMessage());
        }
    }

    public String state(String batchId) {
        try {
            LivyRestClient restClient = new LivyRestClient();
            String statusResult = restClient.livyGetJobStatusBatches(batchId);
            JSONObject stateJson = new JSONObject(statusResult);
            return stateJson.getString("state");
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public Boolean kill(String batchId) {
        try {
            LivyRestClient restClient = new LivyRestClient();
            String statusResult = restClient.livyDeleteBatches(batchId);
            JSONObject stateJson = new JSONObject(statusResult);
            return stateJson.getString("msg").equalsIgnoreCase("deleted")? true: false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private void livyLog(JSONObject logInfo, Logger logger) {
        for (String log: getLogs(logInfo)) {
            logger.log(log);
        }
        logInfo.remove("log");
        logger.log(logInfo.toString());
    }

    private List<String> getLogs(JSONObject logInfo) {
        List<String> logs = Lists.newArrayList();
        if (logInfo.has("log")) {
            try {
                JSONArray logArray = logInfo.getJSONArray("log");

                for (int i=0; i<logArray.length(); i++) {
                    logs.add(logArray.getString(i));
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return logs;
    }

}
