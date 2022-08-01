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

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.CheckStateEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

public class FullGCDurationChecker extends AbstractHealthChecker {
    private static final Logger logger = LoggerFactory.getLogger(FullGCDurationChecker.class);

    private AtomicLong accumulator = new AtomicLong(0);

    private FullGCRecord[] ringBuffer;

    private int factor;
    private long guardianCheckInterval;

    private boolean restartEnabled;
    private double ratioThreshold;

    private boolean busyEnabled;
    private double lowWatermark;
    private double highWatermark;

    public FullGCDurationChecker() {
        setPriority(10000);

        factor = getKylinConfig().getGuardianFullGCCheckFactor();
        guardianCheckInterval = getKylinConfig().getGuardianCheckInterval();

        restartEnabled = getKylinConfig().isFullGCRatioBeyondRestartEnabled();
        ratioThreshold = getKylinConfig().getGuardianFullGCRatioThreshold();

        busyEnabled = getKylinConfig().isDowngradeOnFullGCBusyEnable();
        lowWatermark = getKylinConfig().getGuardianFullGCLowWatermark();
        highWatermark = getKylinConfig().getGuardianFullGCHighWatermark();

        lowWatermark = Double.min(highWatermark, lowWatermark);

        ringBuffer = new FullGCRecord[factor];
    }

    @Override
    CheckResult doCheck() {
        CheckResult result = new CheckResult(CheckStateEnum.NORMAL);

        try {
            double fgcTime = getGCTime();
            long now = getNowTime();
            double fullGCRatio = 0.0;

            int index = (int) (accumulator.get() % ringBuffer.length);
            if (null != ringBuffer[index]) {
                FullGCRecord oldRecord = ringBuffer[index];
                long duration = (now - oldRecord.checkTime) / 1000;
                fullGCRatio = (fgcTime - oldRecord.fgcTime) / duration * 100;

                if (restartEnabled && fullGCRatio >= ratioThreshold) {
                    result = new CheckResult(CheckStateEnum.RESTART,
                            String.format(Locale.ROOT, "Full gc time duration ratio in %d seconds is more than %.2f%%",
                                    factor * guardianCheckInterval, ratioThreshold));
                } else if (busyEnabled) {
                    if (fullGCRatio >= highWatermark) {
                        result = new CheckResult(CheckStateEnum.QUERY_DOWNGRADE,
                                String.format(Locale.ROOT,
                                        "Full gc time duration ratio in %d seconds is more than %.2f%%",
                                        factor * guardianCheckInterval, highWatermark));
                    } else if (fullGCRatio < lowWatermark) {
                        result = new CheckResult(CheckStateEnum.QUERY_UPGRADE,
                                String.format(Locale.ROOT,
                                        "Full gc time duration ratio in %d seconds is less than %.2f%%",
                                        factor * guardianCheckInterval, lowWatermark));
                    }
                }
            }

            logger.info("Full gc time duration ratio in {} seconds is {}%, full gc time: {}",
                    factor * guardianCheckInterval, fullGCRatio, fgcTime);

            ringBuffer[index] = new FullGCRecord(now, fgcTime);
            accumulator.incrementAndGet();
        } catch (Exception e) {
            logger.error("Guardian Process: check full gc time failed!", e);
            result = new CheckResult(CheckStateEnum.WARN, e.getMessage());
        }

        return result;
    }

    @VisibleForTesting
    public long getNowTime() {
        return System.currentTimeMillis();
    }

    @VisibleForTesting
    public double getGCTime() throws ShellException {
        String cmd = "sh " + getKylinHome() + "/sbin/guardian-get-fgc-time.sh";

        CliCommandExecutor.CliCmdExecResult result = getCommandExecutor().execute(cmd, null);
        return Double.parseDouble(result.getCmd().substring(0, result.getCmd().lastIndexOf('\n')));
    }

    @Getter
    @Setter
    @AllArgsConstructor
    private static class FullGCRecord {
        private long checkTime;
        private double fgcTime;
    }
}
