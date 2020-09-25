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
package org.apache.kylin.source.jdbc.sqoop;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SourceConfigurationUtil;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class SqoopCmdStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(SqoopCmdStep.class);
    private final PatternedLogger stepLogger = new PatternedLogger(logger);

    private static final String MR_OVERRIDE_QUEUE_KEY = "mapreduce.job.queuename";
    private static final String DEFAULT_QUEUE = "default";

    public void setCmd(String cmd) {
        setParam("cmd", cmd);
    }

    public SqoopCmdStep() {
    }

    protected void sqoopFlatHiveTable(KylinConfig config) throws IOException {
        String cmd = getParam("cmd");
        cmd = String.format(Locale.ROOT, "%s/bin/sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true "
                + generateSqoopConfigArgString() + cmd, config.getSqoopHome());
        stepLogger.log(String.format(Locale.ROOT, "exe cmd:%s", cmd));
        Pair<Integer, String> response = config.getCliCommandExecutor().execute(cmd, stepLogger, null);
        getManager().addJobInfo(getId(), stepLogger.getInfo());
        if (response.getFirst() != 0) {
            throw new RuntimeException("Failed to create flat hive table, error code " + response.getFirst());
        }
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try {
            sqoopFlatHiveTable(config);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog(), e);
        }
    }

    protected String generateSqoopConfigArgString() {
        KylinConfig kylinConfig = getConfig();
        Map<String, String> config = Maps.newHashMap();
        config.put("mapreduce.job.queuename", getSqoopJobQueueName(kylinConfig)); // override job queue from mapreduce config
        config.putAll(SourceConfigurationUtil.loadSqoopConfiguration());
        config.putAll(kylinConfig.getSqoopConfigOverride());

        StringBuilder args = new StringBuilder();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            args.append(" -D" + entry.getKey() + "=" + entry.getValue() + " ");
        }
        return args.toString();
    }

    protected String getSqoopJobQueueName(KylinConfig config) {
        Map<String, String> mrConfigOverride = config.getMRConfigOverride();
        if (mrConfigOverride.containsKey(MR_OVERRIDE_QUEUE_KEY)) {
            return mrConfigOverride.get(MR_OVERRIDE_QUEUE_KEY);
        }
        return DEFAULT_QUEUE;
    }
}
