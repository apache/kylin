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
package org.apache.kylin.tool.daemon.checker;

import java.util.List;
import java.util.Locale;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.SecretKeyUtil;
import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.CheckStateEnum;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

public class KEStatusChecker extends AbstractHealthChecker {
    public static final String PERMISSION_DENIED = "Check permission failed!";
    private static final Logger logger = LoggerFactory.getLogger(KEStatusChecker.class);
    private int failCount = 0;

    public KEStatusChecker() {
        setPriority(100000);
    }

    /**
     * KE restart, KG have to restart.
     * @return
     */
    private byte[] getEncryptedTokenForKAPHealth() throws Exception {
        try {
            if (null == getKgSecretKey()) {
                setKgSecretKey(SecretKeyUtil.readKGSecretKeyFromFile());
            }

            if (null == getKePid()) {
                setKEPid(ToolUtil.getKylinPid());
            }
            return SecretKeyUtil.generateEncryptedTokenWithPid(getKgSecretKey(), getKePid());
        } catch (Exception e) {
            logger.error("Read KG secret key from file failed.");
            throw e;
        }
    }

    @VisibleForTesting
    public EnvelopeResponse<Status> getHealthStatus() throws Exception {
        TypeReference<EnvelopeResponse<Status>> typeRef = new TypeReference<EnvelopeResponse<Status>>() {
        };
        byte[] encryptedToken = getEncryptedTokenForKAPHealth();
        return getRestClient().getKapHealthStatus(typeRef, encryptedToken);
    }

    @Override
    CheckResult doCheck() {
        try {
            EnvelopeResponse<Status> response = getHealthStatus();
            if (!KylinException.CODE_SUCCESS.equals(response.code)) {
                if (PERMISSION_DENIED.equals(response.getMsg())) {
                    setKgSecretKey(null);
                }

                throw new IllegalStateException("Get KE health status failed: " + response.msg);
            }

            Status status = response.getData();

            StringBuilder sb = new StringBuilder();

            boolean sparkRestart = false;
            boolean slowQueryRestart = false;

            SparkStatus sparkStatus = status.getSparkStatus();
            if (getKylinConfig().isSparkFailRestartKeEnabled()
                    && sparkStatus.getFailureTimes() >= getKylinConfig().getGuardianSparkFailThreshold()) {
                sparkRestart = true;
                sb.append(String.format(Locale.ROOT,
                        "Spark restart failure reach %s times, last restart failure time %s. ",
                        getKylinConfig().getGuardianSparkFailThreshold(), sparkStatus.getLastFailureTime()));
            }

            List<CanceledSlowQueryStatus> slowQueryStatusList = status.getCanceledSlowQueryStatus();
            if (CollectionUtils.isNotEmpty(slowQueryStatusList)) {
                long failedKillQueries = slowQueryStatusList.stream().filter(slowQueryStatus -> slowQueryStatus
                        .getCanceledTimes() >= getKylinConfig().getGuardianSlowQueryKillFailedThreshold()).count();

                if (getKylinConfig().isSlowQueryKillFailedRestartKeEnabled() && failedKillQueries > 0) {
                    slowQueryRestart = true;
                    sb.append(String.format(Locale.ROOT, "Have slowQuery be canceled reach %s times. ",
                            getKylinConfig().getGuardianSparkFailThreshold()));
                }
            }

            if (sparkRestart || slowQueryRestart) {
                return new CheckResult(CheckStateEnum.RESTART, sb.toString());
            }

            failCount = 0;
            return new CheckResult(CheckStateEnum.NORMAL);
        } catch (Exception e) {
            logger.info("Check KE status failed! ", e);

            if (++failCount >= getKylinConfig().getGuardianApiFailThreshold()) {
                return new CheckResult(CheckStateEnum.RESTART, String.format(Locale.ROOT,
                        "Instance is in inaccessible status, API failed count reach %d", failCount));
            } else {
                return new CheckResult(CheckStateEnum.WARN, e.getMessage());
            }
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EnvelopeResponse<T> {
        protected String code;
        protected T data;
        protected String msg;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Status {
        @JsonProperty("spark_status")
        private SparkStatus sparkStatus;

        @JsonProperty("slow_queries_status")
        private List<CanceledSlowQueryStatus> canceledSlowQueryStatus;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SparkStatus {
        @JsonProperty("restart_failure_times")
        private int failureTimes;
        @JsonProperty("last_restart_failure_time")
        private long lastFailureTime;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CanceledSlowQueryStatus {
        @JsonProperty("query_id")
        private String queryId;
        @JsonProperty("canceled_times")
        private int canceledTimes;
        @JsonProperty("last_canceled_time")
        private long lastCanceledTime;
        @JsonProperty("duration_time")
        private float queryDurationTime;
    }
}
