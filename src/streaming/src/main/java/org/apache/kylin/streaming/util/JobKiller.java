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

package org.apache.kylin.streaming.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.cluster.ClusterManagerFactory;
import org.apache.kylin.cluster.IClusterManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.guava30.shaded.common.util.concurrent.UncheckedTimeoutException;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.app.StreamingEntry;
import org.apache.kylin.streaming.app.StreamingMergeEntry;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class JobKiller {
    private static final Logger logger = LoggerFactory.getLogger(JobKiller.class);
    private static final String GREP_CMD = "ps -ef|grep '%s' | grep -v grep|awk '{print $2}'";

    private static boolean isYarnEnv = StreamingUtils.isJobOnCluster(KylinConfig.getInstanceFromEnv());

    private static IClusterManager mock = null;

    public static IClusterManager createClusterManager() {
        if (mock != null) {
            return mock;
        } else {
            return ClusterManagerFactory.create(KylinConfig.getInstanceFromEnv());
        }
    }

    public static boolean applicationExisted(String jobId) {
        boolean existed = false;
        if (isYarnEnv) {
            int errCnt = 0;
            while (errCnt++ < 3) {
                try {
                    final IClusterManager cm = createClusterManager();
                    return cm.applicationExisted(jobId);
                } catch (UncheckedTimeoutException e) {
                    logger.warn(e.getMessage());
                    existed = false;
                }
            }
        }
        return existed;
    }

    public static synchronized void killApplication(String jobId) {
        if (isYarnEnv) {
            int errCnt = 0;
            while (errCnt++ < 3) {
                try {
                    final IClusterManager cm = createClusterManager();
                    if (cm.applicationExisted(jobId)) {
                        cm.killApplication("", jobId);
                        logger.info("kill jobId:{}", jobId);
                    }
                    return;
                } catch (UncheckedTimeoutException e) {
                    logger.warn(e.getMessage());
                }
            }
        }
    }

    /**
     * @param jobMeta
     * @return statusCode value
     *     0: process is killed successfully
     *     1: process is not existed or called from none cluster
     *     negative number: process is kill unsuccessfully
     */
    public static synchronized int killProcess(StreamingJobMeta jobMeta) {
        if (!isYarnEnv) {
            if (jobMeta.getJobType() == JobTypeEnum.STREAMING_BUILD) {
                StreamingEntry.stop();
            } else if (jobMeta.getJobType() == JobTypeEnum.STREAMING_MERGE) {
                StreamingMergeEntry.stop();
            }
            return 1;
        } else {
            val strLogger = new StringLogger();
            val exec = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
            val config = KylinConfig.getInstanceFromEnv();
            if (!StringUtils.isEmpty(jobMeta.getNodeInfo())) {
                String host = jobMeta.getNodeInfo().split(":")[0];
                if (!AddressUtil.isSameHost(host)) {
                    exec.setRunAtRemote(host, config.getRemoteSSHPort(), config.getRemoteSSHUsername(),
                            config.getRemoteSSHPassword());
                } else {
                    exec.setRunAtRemote(null, config.getRemoteSSHPort(), config.getRemoteSSHUsername(),
                            config.getRemoteSSHPassword());
                }
            }
            return killYarnEnvProcess(exec, jobMeta, strLogger);
        }
    }

    public static int killYarnEnvProcess(CliCommandExecutor exec, StreamingJobMeta jobMeta, StringLogger strLogger) {
        String nodeInfo = jobMeta.getNodeInfo();
        int statusCode = -1;

        String jobId = StreamingUtils.getJobId(jobMeta.getModelId(), jobMeta.getJobType().name());
        int retryCnt = 0;

        boolean forced = false;
        while (retryCnt++ < 6) {
            try {
                int errCode = grepProcess(exec, strLogger, jobId);
                if (errCode == 0) {
                    if (!strLogger.getContents().isEmpty()) {
                        if (retryCnt >= 3) {
                            forced = true;
                        }
                        statusCode = doKillProcess(exec, jobId, forced);
                    } else {
                        statusCode = 1;
                        break;
                    }
                }
                StreamingUtils.sleep(1000L * retryCnt);

            } catch (ShellException e) {
                logger.warn("failed to kill driver {} on {}", nodeInfo, jobMeta.getProcessId());
            }
        }
        return statusCode;
    }

    public static int grepProcess(CliCommandExecutor exec, StringLogger strLogger, String jobId) throws ShellException {
        String cmd = String.format(Locale.getDefault(), GREP_CMD, jobId);
        val result = exec.execute(cmd, strLogger).getCode();
        logger.info("grep process cmd={}, result ={} ", cmd, result);
        return result;
    }

    public static int doKillProcess(CliCommandExecutor exec, String jobId, boolean forced) throws ShellException {
        String cmd = String.format(Locale.getDefault(), GREP_CMD, jobId);
        val force = forced ? " -9" : StringUtils.EMPTY;
        val result = exec.execute(cmd + "|xargs kill" + force, null).getCode();
        logger.info("kill process cmd={}, result ={} ", cmd, result);
        return result;
    }

    static class StringLogger implements org.apache.kylin.common.util.Logger {
        private List<String> contents = new ArrayList<>(2);

        @Override
        public void log(String message) {
            if (!StringUtils.isEmpty(message)) {
                contents.add(message);
            }
        }

        public List<String> getContents() {
            return contents;
        }
    }
}
