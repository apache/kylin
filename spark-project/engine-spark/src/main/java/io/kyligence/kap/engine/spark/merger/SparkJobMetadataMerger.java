/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.merger;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.cleanup.SnapshotChecker;
import io.kyligence.kap.engine.spark.utils.FileNames;
import io.kyligence.kap.engine.spark.utils.HDFSUtils;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.Getter;

public abstract class SparkJobMetadataMerger extends MetadataMerger {
    private static final Logger log = LoggerFactory.getLogger(SparkJobMetadataMerger.class);
    @Getter
    private final String project;

    protected SparkJobMetadataMerger(KylinConfig config, String project) {
        super(config);
        this.project = project;
    }

    @Override
    public NDataLayout[] merge(String dataflowId, Set<String> segmentIds, Set<Long> layoutIds,
            ResourceStore remoteResourceStore, JobTypeEnum jobType) {
        return new NDataLayout[0];
    }

    protected void recordDownJobStats(AbstractExecutable buildTask, NDataLayout[] addOrUpdateCuboids) {
        String model = buildTask.getTargetSubject();
        long buildEndTime = buildTask.getParent().getEndTime();
        long duration = buildTask.getParent().getDuration();
        long byteSize = 0;
        for (NDataLayout dataCuboid : addOrUpdateCuboids) {
            byteSize += dataCuboid.getByteSize();
        }
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ZoneId zoneId = TimeZone.getTimeZone(kylinConfig.getTimeZone()).toZoneId();
        LocalDate localDate = Instant.ofEpochMilli(buildEndTime).atZone(zoneId).toLocalDate();
        long startOfDay = localDate.atStartOfDay().atZone(zoneId).toInstant().toEpochMilli();
        // update
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(kylinConfig,
                buildTask.getProject());
        jobStatisticsManager.updateStatistics(startOfDay, model, duration, byteSize);
    }

    protected void updateSnapshotTableIfNeed(NDataSegment segment) {
        try {
            log.info("Check snapshot for segment: {}", segment);
            Map<Path, SnapshotChecker> snapshotCheckerMap = new HashMap<>();
            List<TableDesc> needUpdateTableDescs = new ArrayList<>();
            Map<String, String> snapshots = segment.getSnapshots();
            NTableMetadataManager manager = NTableMetadataManager.getInstance(getConfig(), segment.getProject());
            KylinConfig segmentConf = segment.getConfig();
            String workingDirectory = KapConfig.wrap(segmentConf).getReadHdfsWorkingDirectory();
            for (Map.Entry<String, String> entry : snapshots.entrySet()) {
                TableDesc tableDesc = manager.getTableDesc(entry.getKey());
                Path snapshotPath = FileNames.snapshotFileWithWorkingDir(tableDesc, workingDirectory);
                FileStatus lastFile = HDFSUtils.findLastFile(snapshotPath);
                FileStatus segmentFile = HDFSUtils.getFileStatus(new Path(workingDirectory + entry.getValue()));

                FileStatus currentFile = null;
                if (tableDesc.getLastSnapshotPath() != null) {
                    currentFile = HDFSUtils.getFileStatus(new Path(workingDirectory + tableDesc.getLastSnapshotPath()));
                }

                if (lastFile.getModificationTime() <= segmentFile.getModificationTime()) {

                    log.info("Update snapshot table {} : from {} to {}", entry.getKey(),
                            currentFile == null ? 0L : currentFile.getModificationTime(),
                            lastFile.getModificationTime());
                    log.info("Update snapshot table {} : from {} to {}", entry.getKey(),
                            currentFile == null ? "null" : currentFile.getPath(), segmentFile.getPath());
                    TableDesc copyDesc = manager.copyForWrite(tableDesc);
                    copyDesc.setLastSnapshotPath(entry.getValue());
                    needUpdateTableDescs.add(copyDesc);
                    snapshotCheckerMap.put(snapshotPath, new SnapshotChecker(segmentConf.getSnapshotMaxVersions(),
                            segmentConf.getSnapshotVersionTTL(), segmentFile.getModificationTime()));
                } else {
                    log.info(
                            "Skip update snapshot table because current segment snapshot table is to old. Current segment snapshot table ts is: {}",
                            segmentFile.getModificationTime());
                }
            }
            UnitOfWork.doInTransactionWithRetry(() -> {
                NTableMetadataManager updateManager = NTableMetadataManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), segment.getProject());
                for (TableDesc tableDesc : needUpdateTableDescs) {
                    updateManager.updateTableDesc(tableDesc);
                }
                return null;
            }, segment.getProject(), 1);
            for (Map.Entry<Path, SnapshotChecker> entry : snapshotCheckerMap.entrySet()) {
                HDFSUtils.deleteFilesWithCheck(entry.getKey(), entry.getValue());
            }
        } catch (Throwable th) {
            log.error("Error for update snapshot table", th);
        }
    }
}
