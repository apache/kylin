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
package org.apache.kylin.common.util;

import static org.apache.kylin.common.exception.SystemErrorCode.FAILED_MERGE_SEGMENT;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentMergeStorageChecker {
    private static final Logger logger = LoggerFactory.getLogger(SegmentMergeStorageChecker.class);

    private static final Path ROOT_PATH = new Path("/");

    private static final ThreadLocal<FileSystem> rootFileSystem = new ThreadLocal<>();

    public static void setRootFileSystem(FileSystem fileSystem) {
        rootFileSystem.set(fileSystem);
    }

    private static FileSystem getRootFileSystem(Configuration conf) {
        if (rootFileSystem.get() == null) {
            return HadoopUtil.getFileSystem(ROOT_PATH, conf);
        }
        return rootFileSystem.get();
    }

    public static void checkMergeSegmentThreshold(KylinConfig kylinConfig, String workingDir, long expectedSpaceByByte)
            throws KylinException {
        Double thresholdValue = kylinConfig.getMergeSegmentStorageThreshold();
        if (thresholdValue.equals(Double.parseDouble("0"))) {
            return;
        }

        logger.info("HDFS threshold check has been enabled, threshold value is {}", thresholdValue);
        Configuration hadoopConf = getHadoopConfiguration(kylinConfig, workingDir);
        int dfsReplication = getDfsReplication(workingDir, hadoopConf);
        try {
            checkClusterStorageThresholdValue(workingDir, hadoopConf, expectedSpaceByByte, thresholdValue,
                    dfsReplication);
        } catch (Exception ex) {
            logger.error("Failed to check cluster storage threshold value.", ex);
            if (!(ex instanceof KylinException)) {
                throw (KylinException) ex;
            }
            throw new KylinException(FAILED_MERGE_SEGMENT, MsgPicker.getMsg().getSegmentMergeStorageCheckError(), ex);
        }
    }

    public static Configuration getHadoopConfiguration(KylinConfig kylinConfig, String workingDir) {
        Configuration hadoopConf = HadoopUtil.getCurrentConfiguration();
        logger.info("build conf is {}, working dir is :{}", kylinConfig.getBuildConf(), workingDir);
        if (!kylinConfig.getBuildConf().isEmpty()) {
            hadoopConf = HadoopUtil.healSickConfig(new Configuration());
            hadoopConf.set("fs.defaultFS", workingDir);
        }
        return hadoopConf;
    }

    public static int getDfsReplication(String workingDir, Configuration hadoopConf) {
        Configuration conf = HadoopUtil.getHadoopConfFromSparkEngine();
        String replicationValue = conf.get("dfs.replication");
        if (replicationValue.isEmpty()) {
            FileSystem fileSystem = HadoopUtil.getFileSystem(new Path(workingDir), hadoopConf);
            return fileSystem.getDefaultReplication(new Path(workingDir));
        }
        return Integer.parseInt(replicationValue);

    }

    public static void checkClusterStorageThresholdValue(String workingDir, Configuration conf,
            Long expectedSpaceByByte, double thresholdValue, int replication) throws IOException {
        HadoopSpaceInfo spaceInfoStatus = HadoopSpaceInfo.getHadoopSpaceInfo(conf, workingDir);
        logger.info("HDFS cluster space usage: {} B total space, {} B used space, and {} B remaining space.",
                spaceInfoStatus.getTotalSpace(), spaceInfoStatus.getUsedSpace(), spaceInfoStatus.getRemainingSpace());
        if (spaceInfoStatus.getTotalSpace() <= 0) {
            logger.error("The HDFS cluster storage space is insufficient.");
            throw new KylinException(FAILED_MERGE_SEGMENT, MsgPicker.getMsg().getSegmentMergeStorageCheckError());
        }

        expectedSpaceByByte = recountExpectedSpaceByte(expectedSpaceByByte, replication);
        if (isThresholdAlarms(expectedSpaceByByte, spaceInfoStatus.getRemainingSpace(), spaceInfoStatus.getTotalSpace(),
                thresholdValue)) {
            logger.error(
                    "Failed to merge segment because the HDFS cluster usage exceeds the threshold after the merge.");
            throw new KylinException(FAILED_MERGE_SEGMENT, MsgPicker.getMsg().getSegmentMergeStorageCheckError());
        }
    }

    public static long recountExpectedSpaceByte(long originalExpectedSpaceByte, int replication) {
        logger.info("Merging segments requires {} B space(original)", originalExpectedSpaceByte);
        logger.info("Kap replication is {}", replication);
        long expectedSpaceByByte = originalExpectedSpaceByte * replication;
        logger.info("Merging segments requires {} B space after recount.", expectedSpaceByByte);
        return expectedSpaceByByte;
    }

    public static boolean isThresholdAlarms(double expectedSpaceByByte, double remainingSpace, double totalSpace,
            double thresholdValue) {
        logger.info("The space utilization is expected to be {}% after the merge.",
                (1 - (remainingSpace - expectedSpaceByByte) / totalSpace) * 100);
        return (1 - ((remainingSpace - expectedSpaceByByte) / totalSpace)) >= thresholdValue;
    }

    public static Path getSpaceQuotaPath(FileSystem fileSystem, Path path) {
        try {
            if (path.getName().isEmpty()) {
                return null;
            }
            ContentSummary contentSummary = fileSystem.getContentSummary(path);
            long spaceQuota = contentSummary.getSpaceQuota();
            if (spaceQuota == -1 && path.getParent() != null) {
                return getSpaceQuotaPath(fileSystem, path.getParent());
            } else if (spaceQuota != -1) {
                return path;
            }
        } catch (IOException ex) {
            logger.error("Get space quota path error:{}", ex.getMessage(), ex);
        }
        return null;
    }

    static class HadoopSpaceInfo {
        private long usedSpace;
        private long totalSpace;
        private long remainingSpace;

        private HadoopSpaceInfo() {
        }

        public static HadoopSpaceInfo getHadoopSpaceInfo(Configuration conf, String workingDir) throws IOException {
            HadoopSpaceInfo hadoopSpaceInfo = new HadoopSpaceInfo();
            long usedSpace;
            long totalSpace;
            long remainingSpace;
            FileSystem fileSystem = getRootFileSystem(conf);
            FsStatus status = fileSystem.getStatus();
            if (fileSystem.exists(new Path(workingDir))) {
                Path spaceQuotaPath = getSpaceQuotaPath(fileSystem, new Path(workingDir));
                logger.info("The space quota path is {}", spaceQuotaPath);
                if (spaceQuotaPath != null) {
                    logger.info("Indicates that quota is configured.");
                    ContentSummary contentSummary = fileSystem.getContentSummary(spaceQuotaPath);
                    long spaceQuota = contentSummary.getSpaceQuota();
                    usedSpace = contentSummary.getSpaceConsumed();
                    remainingSpace = spaceQuota - contentSummary.getSpaceConsumed();
                    totalSpace = spaceQuota;
                } else {
                    logger.info("Indicates that quota is not configured.");
                    usedSpace = status.getUsed();
                    remainingSpace = status.getRemaining();
                    totalSpace = status.getCapacity();
                }
            } else {
                usedSpace = status.getUsed();
                remainingSpace = status.getRemaining();
                totalSpace = status.getCapacity();
            }
            hadoopSpaceInfo.setRemainingSpace(remainingSpace);
            hadoopSpaceInfo.setTotalSpace(totalSpace);
            hadoopSpaceInfo.setUsedSpace(usedSpace);
            return hadoopSpaceInfo;
        }

        public long getUsedSpace() {
            return usedSpace;
        }

        public long getTotalSpace() {
            return totalSpace;
        }

        public long getRemainingSpace() {
            return remainingSpace;
        }

        public void setUsedSpace(long usedSpace) {
            this.usedSpace = usedSpace;
        }

        public void setTotalSpace(long totalSpace) {
            this.totalSpace = totalSpace;
        }

        public void setRemainingSpace(long remainingSpace) {
            this.remainingSpace = remainingSpace;
        }
    }
}
