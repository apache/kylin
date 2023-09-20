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
package org.apache.kylin.metadata.sourceusage;

import static org.apache.kylin.common.exception.CommonErrorCode.LICENSE_OVER_CAPACITY;
import static org.apache.kylin.metadata.sourceusage.SourceUsageRecord.CapacityStatus.OVERCAPACITY;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.constant.Constants;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.mail.MailNotificationType;
import org.apache.kylin.common.mail.MailNotifier;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.SizeConvertUtil;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cube.model.NCubeJoinedFlatTableDesc;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.sourceusage.SourceUsageRecord.CapacityStatus;
import org.apache.kylin.metadata.sourceusage.SourceUsageRecord.ProjectCapacityDetail;
import org.apache.kylin.metadata.sourceusage.mail.SourceUsageMailUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class SourceUsageManager {

    private static final Logger logger = LoggerFactory.getLogger(SourceUsageManager.class);

    public static SourceUsageManager getInstance(KylinConfig config) {
        return config.getManager(SourceUsageManager.class);
    }

    // called by reflection
    static SourceUsageManager newInstance(KylinConfig config) {
        return new SourceUsageManager(config);
    }

    // ============================================================================

    final KylinConfig config;

    private final CachedCrudAssist<SourceUsageRecord> crud;

    private SourceUsageManager(KylinConfig config) {
        this.config = config;
        this.crud = new CachedCrudAssist<SourceUsageRecord>(getStore(), ResourceStore.HISTORY_SOURCE_USAGE,
                SourceUsageRecord.class) {
            @Override
            protected SourceUsageRecord initEntityAfterReload(SourceUsageRecord entity, String resourceName) {
                entity.setResPath(concatResourcePath(resourceName));
                return entity;
            }
        };
    }

    public SourceUsageRecord getSourceUsageRecord(String resourceName) {
        return this.crud.listAll().stream()
                .filter(usage -> usage.getResourcePath().equalsIgnoreCase(concatResourcePath(resourceName))).findAny()
                .orElse(null);
    }

    public interface SourceUsageRecordUpdater {
        void modify(SourceUsageRecord record);
    }

    public SourceUsageRecord copy(SourceUsageRecord df) {
        return crud.copyBySerialization(df);
    }

    public SourceUsageRecord createSourceUsageRecord(String resourceName, SourceUsageRecord record) {
        record.setResPath(concatResourcePath(resourceName));
        return crud.save(record);
    }

    public SourceUsageRecord updateSourceUsageRecord(String resourceName, SourceUsageRecordUpdater updater) {
        SourceUsageRecord record = getSourceUsageRecord(resourceName);
        if (record == null) {
            record = new SourceUsageRecord();
        }
        SourceUsageRecord copy = copy(record);
        updater.modify(copy);
        return crud.save(copy);
    }

    public static String concatResourcePath(String resourceName) {
        return ResourceStore.HISTORY_SOURCE_USAGE + "/" + resourceName + MetadataConstants.FILE_SURFIX;
    }

    public Map<String, Long> calcAvgColumnSourceBytes(NDataSegment segment) {
        Map<String, Long> columnSourceBytes = Maps.newHashMap();
        Set<TblColRef> usedColumns;
        try {
            usedColumns = new NCubeJoinedFlatTableDesc(segment).getUsedColumns();
        } catch (Exception e) {
            return columnSourceBytes;
        }
        long inputRecordsSize = segment.getSourceBytesSize();
        if (inputRecordsSize == -1) {
            logger.debug("Source bytes size for segment: {} is -1", segment);
            return columnSourceBytes;
        }
        if (usedColumns.isEmpty()) {
            logger.debug("No effective columns found in segment: {}", segment);
            return columnSourceBytes;
        }
        List<NDataModel.NamedColumn> allColumns = Lists
                .newArrayList(segment.getModel().getEffectiveNamedColumns().values());
        int columnSize = allColumns.isEmpty() ? usedColumns.size() : allColumns.size();
        // all named columns as denominator, since inputRecordsSize includes all cols on table
        long perColumnSize = inputRecordsSize / columnSize;
        for (TblColRef tblColRef : usedColumns) {
            columnSourceBytes.put(tblColRef.getCanonicalName(), perColumnSize);
        }
        return columnSourceBytes;
    }

    @VisibleForTesting
    public Map<String, Long> sumDataflowColumnSourceMap(NDataflow dataflow) {
        Map<String, Long> dataflowSourceMap = new HashMap<>();
        Segments<NDataSegment> segments = dataflow.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        List<NDataSegment> oldSegments = Lists.newArrayList();
        for (NDataSegment segment : segments) {
            Map<String, Long> columnSourceBytesMap = segment.getColumnSourceBytes();
            if (MapUtils.isEmpty(columnSourceBytesMap)) {
                oldSegments.add(segment);
            } else {
                for (Map.Entry<String, Long> sourceMap : columnSourceBytesMap.entrySet()) {
                    String column = sourceMap.getKey();
                    long value = dataflowSourceMap.getOrDefault(column, 0L);
                    dataflowSourceMap.put(column, sourceMap.getValue() + value);
                }
            }
        }
        if (!oldSegments.isEmpty()) {
            List<NDataSegment> evaluations = isPartitioned(dataflow) ? oldSegments // all old segments
                    : Lists.newArrayList(oldSegments.get(oldSegments.size() - 1)); // last old segment
            for (NDataSegment segment : evaluations) {
                Map<String, Long> estimateSourceMap = calcAvgColumnSourceBytes(segment);
                for (Map.Entry<String, Long> sourceMap : estimateSourceMap.entrySet()) {
                    String column = sourceMap.getKey();
                    long value = dataflowSourceMap.getOrDefault(column, 0L);
                    dataflowSourceMap.put(column, sourceMap.getValue() + value);
                }
            }
        }
        return dataflowSourceMap;
    }

    private boolean isPartitioned(NDataflow dataflow) {
        val partDesc = dataflow.getModel().getPartitionDesc();
        if (Objects.isNull(partDesc)) {
            return false;
        }
        val colRef = partDesc.getPartitionDateColumnRef();
        if (Objects.isNull(colRef)) {
            return false;
        }
        return colRef.getColumnDesc().isPartitioned();
    }

    public SourceUsageRecord updateSourceUsage(SourceUsageRecord sourceUsageRecord) {
        createOrUpdate(sourceUsageRecord);
        return sourceUsageRecord;
    }

    private void createOrUpdate(SourceUsageRecord usageRecord) {
        try {
            String resourceName = usageRecord.resourceName();
            if (resourceName == null) {
                resourceName = DateFormat.formatToCompactDateStr(System.currentTimeMillis());
            }
            SourceUsageRecord record = getSourceUsageRecord(resourceName);
            if (record == null) {
                record = createSourceUsageRecord(resourceName, usageRecord);
            } else {
                record = updateSourceUsageRecord(resourceName, copyForWrite -> {
                    copyForWrite.setLicenseCapacity(usageRecord.getLicenseCapacity());
                    copyForWrite.setCapacityDetails(usageRecord.getCapacityDetails());
                    copyForWrite.setCapacityStatus(usageRecord.getCapacityStatus());
                    copyForWrite.setCheckTime(usageRecord.getCheckTime());
                    copyForWrite.setCurrentCapacity(usageRecord.getCurrentCapacity());
                    if (!isOverCapacityThreshold(copyForWrite) && !copyForWrite.isCapacityNotification()) {
                        copyForWrite.setCapacityNotification(true);
                        logger.info("Capacity usage is less than threshold, enable notification");
                    } else if (copyForWrite.isCapacityNotification() && config.isOverCapacityNotificationEnabled()
                            && isOverCapacityThreshold(copyForWrite)) {
                        boolean isSuccessful = MailNotifier.notifyUser(config,
                                SourceUsageMailUtil.createMail(MailNotificationType.OVER_LICENSE_CAPACITY_THRESHOLD,
                                        copyForWrite.getLicenseCapacity(), copyForWrite.getCurrentCapacity()),
                                Lists.newArrayList(config.getOverCapacityMailingList()));
                        if (isSuccessful) {
                            copyForWrite.setCapacityNotification(false);
                            logger.info("Capacity usage is more than threshold and notify user, disable notification");
                        }
                    }
                });
            }
            usageRecord.setCapacityNotification(record.isCapacityNotification());
        } catch (Exception e) {
            logger.error("Failed to update source usage record.", e);
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    /**
     * Read external data source to refresh lookup table row count for a table.
     */
    public void refreshLookupTableRowCount(final TableDesc tableDesc, String project) {
        // no this case currently, because no need to calculate fact table and lookup table without snapshot
    }

    public SourceUsageRecord getLatestRecord() {
        int oneDay = 24;

        // try to reduce the scan range
        SourceUsageRecord r;
        r = getLatestRecord(oneDay);
        if (r == null)
            r = getLatestRecord(oneDay * 7);
        if (r == null)
            r = getLatestRecord(oneDay * 31);
        if (r == null)
            return getLatestRecord(oneDay * getThresholdByDayFromOrigin());

        return r;
    }

    public SourceUsageRecord getLatestRecord(int hoursAgo) {
        List<SourceUsageRecord> recordList = getLatestRecordByHours(hoursAgo);
        if (CollectionUtils.isEmpty(recordList)) {
            return null;
        }
        return Collections.max(recordList, (o1, o2) -> {
            long comp = o1.getLastModified() - o2.getLastModified();
            if (comp == 0) {
                return 0;
            }
            return comp < 0 ? -1 : 1;
        });
    }

    public List<SourceUsageRecord> getLatestRecordByMs(long msAgo) {
        long from = System.currentTimeMillis() - msAgo;
        return this.crud.listAll().stream().filter(usage -> usage.getCreateTime() >= from).collect(Collectors.toList());
    }

    public List<SourceUsageRecord> getAllRecords() {
        return this.crud.listAll();
    }

    // no init means only use json object
    public List<SourceUsageRecord> getAllRecordsWithoutInit() {
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        val allResourcePaths = resourceStore.listResources(ResourceStore.HISTORY_SOURCE_USAGE);
        if (allResourcePaths == null) {
            return Lists.newArrayList();
        }
        return allResourcePaths.stream().map(path -> getRecordWithoutInit(path)).collect(Collectors.toList());
    }

    private SourceUsageRecord getRecordWithoutInit(String path) {
        try {
            val resource = ResourceStore.getKylinMetaStore(config).getResource(path);
            SourceUsageRecord record = JsonUtil.readValue(resource.getByteSource().read(), SourceUsageRecord.class);
            record.setMvcc(resource.getMvcc());
            return record;
        } catch (IOException e) {
            throw new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, e);
        }
    }

    public void delSourceUsage(String resourceName) {
        this.crud.delete(resourceName);
    }

    public List<SourceUsageRecord> getLatestRecordByHours(int hoursAgo) {
        return getLatestRecordByMs(hoursAgo * 3600L * 1000L);
    }

    public List<SourceUsageRecord> getLatestRecordByDays(int daysAgo) {
        return getLatestRecordByHours(daysAgo * 24);
    }

    private int getThresholdByDayFromOrigin() {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd",
                    Locale.getDefault(Locale.Category.FORMAT));
            Date date = simpleDateFormat.parse("1970-01-01");
            return (int) ((System.currentTimeMillis() - date.getTime()) / (1000 * 60 * 60 * 24));
        } catch (ParseException e) {
            logger.error("parser date error", e);
            return 365 * 10;
        }
    }

    // return all records in last one month
    public List<SourceUsageRecord> getLastMonthRecords() {
        return getLatestRecordByDays(30);
    }

    public List<SourceUsageRecord> getLastQuarterRecords() {
        return getLatestRecordByDays(90);
    }

    public List<SourceUsageRecord> getLastYearRecords() {
        return getLatestRecordByDays(365);
    }

    private boolean isNotOk(SourceUsageRecord.CapacityStatus status) {
        return SourceUsageRecord.CapacityStatus.TENTATIVE == status || SourceUsageRecord.CapacityStatus.ERROR == status;
    }

    private void setNodeInfo(LicenseInfo info) {
        //node part
        List<String> servers = config.getAllServers();
        int currentNodes = servers.size();
        info.setCurrentNode(currentNodes);

        String licenseNodes = System.getProperty(Constants.KE_LICENSE_NODES);
        if (Constants.UNLIMITED.equals(licenseNodes)) {
            info.setNode(-1);
        } else if (!StringUtils.isEmpty(licenseNodes)) {
            try {
                int maximumNodeNums = Integer.parseInt(licenseNodes);
                info.setNode(maximumNodeNums);

                if (maximumNodeNums < currentNodes) {
                    info.setNodeStatus(SourceUsageRecord.CapacityStatus.OVERCAPACITY);
                }
            } catch (NumberFormatException e) {
                logger.error("Illegal value of config ke.license.nodes", e);
            }
        }
    }

    private void setSourceUsageInfo(LicenseInfo info, String project) {
        //capacity part
        SourceUsageRecord latestHistory = getLatestRecord();

        if (latestHistory != null) {
            info.setTime(latestHistory.getCheckTime());

            if (project == null) {
                info.setCurrentCapacity(latestHistory.getCurrentCapacity());
                info.setCapacity(latestHistory.getLicenseCapacity());
                info.setCapacityStatus(latestHistory.getCapacityStatus());
            } else {
                ProjectCapacityDetail projectCapacity = latestHistory.getProjectCapacity(project);

                if (projectCapacity != null) {
                    info.setCurrentCapacity(projectCapacity.getCapacity());
                    info.setCapacity(projectCapacity.getLicenseCapacity());
                    info.setCapacityStatus(projectCapacity.getStatus());
                }
            }

            if (isNotOk(latestHistory.getCapacityStatus())) {
                List<SourceUsageRecord> recentHistories = SourceUsageManager.getInstance(config).getLastMonthRecords();

                info.setFirstErrorTime(latestHistory.getCheckTime());
                for (int i = recentHistories.size() - 1; i >= 0; i--) {
                    SourceUsageRecord historyRecord = recentHistories.get(i);
                    if (isNotOk(historyRecord.getCapacityStatus())) {
                        info.setFirstErrorTime(historyRecord.getCheckTime());
                    } else {
                        break;
                    }
                }
            }
        } else {
            logger.warn("Latest history of source usage record is null.");
        }
    }

    private LicenseInfo getLicenseInfo(String project) {
        LicenseInfo info = new LicenseInfo();

        setNodeInfo(info);
        setSourceUsageInfo(info, project);

        long firstErrorTime = info.getFirstErrorTime();
        if (firstErrorTime != 0L) {
            long dayThreshold = (System.currentTimeMillis() - firstErrorTime) / (1000 * 60 * 60 * 24);
            if (dayThreshold >= 30) {
                logger.warn("Failed to fetch data volume usage for over {} days", dayThreshold);
            }
        }
        return info;
    }

    public void checkIsOverCapacity(String project) {
        if (config.isUTEnv()) {
            return;
        }

        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        if (!kapConfig.isRecordSourceUsage()) {
            logger.info("Skip check over capacity.");
            return;
        }

        boolean checkProject = false;

        if (project != null) {
            KylinConfig kylinConfig = NProjectManager.getInstance(config).getProject(project).getConfig();

            // have project usage capacity config
            if (kylinConfig.getSourceUsageQuota() != -1) {
                checkProject = true;
            }
        }

        LicenseInfo info = getLicenseInfo(checkProject ? project : null);
        CapacityStatus capacityStatus = info.getCapacityStatus();
        if (isNotOk(capacityStatus)) {
            logger.warn("Capacity status is not ok: {}, will not block build job", capacityStatus);
            return;
        }

        String currentCapacity = SizeConvertUtil.byteCountToDisplaySize(info.getCurrentCapacity());
        String capacity = SizeConvertUtil.byteCountToDisplaySize(info.getCapacity());

        if (checkProject) {
            if (info.getCapacityStatus() == OVERCAPACITY && info.getNodeStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getlicenseProjectSourceNodesOverCapacity(),
                                currentCapacity, capacity, info.getCurrentNode(), info.getNode()));
            } else if (info.getCapacityStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getLicenseProjectSourceOverCapacity(), currentCapacity, capacity));
            } else if (info.getNodeStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getLicenseNodesOverCapacity(), info.getCurrentNode(), info.getNode()));
            }
            logger.info("Current capacity status of project: {} is ok", project);
        } else {
            if (info.getCapacityStatus() == OVERCAPACITY && info.getNodeStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getLicenseSourceNodesOverCapacity(),
                                currentCapacity, capacity, info.getCurrentNode(), info.getNode()));
            } else if (info.getCapacityStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getLicenseSourceOverCapacity(), currentCapacity, capacity));
            } else if (info.getNodeStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getLicenseNodesOverCapacity(), info.getCurrentNode(), info.getNode()));
            }
            logger.info("Current capacity status is ok");
        }
    }

    public boolean isOverCapacityThreshold(SourceUsageRecord sourceUsageRecord) {
        if (Constants.UNLIMITED.equals(System.getProperty(Constants.KE_LICENSE_VOLUME))) {
            logger.info("Current license has unlimited volume.");
            return false;
        }
        if (sourceUsageRecord == null) {
            logger.debug("Source usage record is null, ignore...");
            return false;
        }
        long currentCapacity = sourceUsageRecord.getCurrentCapacity();
        long totalCapacity = sourceUsageRecord.getLicenseCapacity();
        logger.info("Current capacity is: {}, total capacity is: {}", currentCapacity, totalCapacity);
        return currentCapacity > totalCapacity * config.getOverCapacityThreshold();
    }

    public <T> T licenseCheckWrap(String project, Callback<T> f) {
        checkIsOverCapacity(project);

        return f.process();
    }

    public interface Callback<T> {
        T process();
    }

    public double calculateRatio(long amount, long totalAmount) {
        if (amount > 0d) {
            // Keep two decimals
            return (Math.round(((double) amount) / totalAmount * 100d)) / 100d;
        }
        return 0d;
    }
}
