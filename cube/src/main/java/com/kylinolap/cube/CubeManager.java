/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.cube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.common.restclient.SingleValueCache;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.cube.model.CubeBuildTypeEnum;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.DimensionDesc;
import com.kylinolap.dict.DateStrDictionary;
import com.kylinolap.dict.Dictionary;
import com.kylinolap.dict.DictionaryInfo;
import com.kylinolap.dict.DictionaryManager;
import com.kylinolap.dict.lookup.HiveTable;
import com.kylinolap.dict.lookup.LookupStringTable;
import com.kylinolap.dict.lookup.SnapshotManager;
import com.kylinolap.dict.lookup.SnapshotTable;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.SegmentStatusEnum;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.metadata.project.ProjectManager;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.metadata.realization.IRealizationProvider;
import com.kylinolap.metadata.realization.RealizationStatusEnum;
import com.kylinolap.metadata.realization.RealizationType;

/**
 * @author yangli9
 */
public class CubeManager implements IRealizationProvider {

    private static String ALPHA_NUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static int HBASE_TABLE_LENGTH = 10;
    private static final Serializer<CubeInstance> CUBE_SERIALIZER = new JsonSerializer<CubeInstance>(CubeInstance.class);

    private static final Logger logger = LoggerFactory.getLogger(CubeManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, CubeManager> CACHE = new ConcurrentHashMap<KylinConfig, CubeManager>();

    public static CubeManager getInstance(KylinConfig config) {
        CubeManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (CubeManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new CubeManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one cubemanager singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeManager from " + config, e);
            }
        }
    }

    public static synchronized void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }

    // ============================================================================

    private KylinConfig config;
    // cube name ==> CubeInstance
    private SingleValueCache<String, CubeInstance> cubeMap = new SingleValueCache<String, CubeInstance>(Broadcaster.TYPE.CUBE);
    // "table/column" ==> lookup table
    private SingleValueCache<String, LookupStringTable> lookupTables = new SingleValueCache<String, LookupStringTable>(Broadcaster.TYPE.METADATA);

    // for generation hbase table name of a new segment
    private HashSet<String> usedStorageLocation = new HashSet<String>();

    private CubeManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeManager with config " + config);
        this.config = config;

        loadAllCubeInstance();
    }

    public List<CubeInstance> listAllCubes() {
        return new ArrayList<CubeInstance>(cubeMap.values());
    }

    public CubeInstance getCube(String cubeName) {
        cubeName = cubeName.toUpperCase();
        return cubeMap.get(cubeName);
    }

    /**
     * Get related Cubes by cubedesc name. By default, the desc name will be
     * translated into upper case.
     *
     * @param descName CubeDesc name
     * @return
     */
    public List<CubeInstance> getCubesByDesc(String descName) {

        descName = descName.toUpperCase();
        List<CubeInstance> list = listAllCubes();
        List<CubeInstance> result = new ArrayList<CubeInstance>();
        Iterator<CubeInstance> it = list.iterator();
        while (it.hasNext()) {
            CubeInstance ci = it.next();
            if (descName.equals(ci.getDescName())) {
                result.add(ci);
            }
        }
        return result;
    }

    public DictionaryInfo buildDictionary(CubeSegment cubeSeg, TblColRef col, String factColumnsPath) throws IOException {
        CubeDesc cubeDesc = cubeSeg.getCubeDesc();
        if (!cubeDesc.getRowkey().isUseDictionary(col))
            return null;

        DictionaryManager dictMgr = getDictionaryManager();
        DictionaryInfo dictInfo = dictMgr.buildDictionary(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, factColumnsPath);
        cubeSeg.putDictResPath(col, dictInfo.getResourcePath());

        saveResource(cubeSeg.getCubeInstance());

        return dictInfo;
    }

    /**
     * return null if no dictionary for given column
     */
    public Dictionary<?> getDictionary(CubeSegment cubeSeg, TblColRef col) {
        DictionaryInfo info = null;
        try {
            DictionaryManager dictMgr = getDictionaryManager();
            // logger.info("Using metadata url " + metadataUrl +
            // " for DictionaryManager");
            String dictResPath = cubeSeg.getDictResPath(col);
            if (dictResPath == null)
                return null;

            info = dictMgr.getDictionaryInfo(dictResPath);
            if (info == null)
                throw new IllegalStateException("No dictionary found by " + dictResPath + ", invalid cube state; cube segment" + cubeSeg + ", col " + col);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get dictionary for cube segment" + cubeSeg + ", col" + col, e);
        }

        return info.getDictionaryObject();
    }

    public SnapshotTable buildSnapshotTable(CubeSegment cubeSeg, String lookupTable) throws IOException {
        MetadataManager metaMgr = getMetadataManager();
        SnapshotManager snapshotMgr = getSnapshotManager();

        HiveTable hiveTable = new HiveTable(metaMgr, lookupTable);
        TableDesc tableDesc = metaMgr.getTableDesc(lookupTable);
        SnapshotTable snapshot = snapshotMgr.buildSnapshot(hiveTable, tableDesc);

        cubeSeg.putSnapshotResPath(lookupTable, snapshot.getResourcePath());

        saveResource(cubeSeg.getCubeInstance());

        return snapshot;
    }

    // sync on update
    public CubeInstance dropCube(String cubeName, boolean deleteDesc) throws IOException {
        logger.info("Dropping cube '" + cubeName + "'");
        // load projects before remove cube from project

        ResourceStore store = getStore();

        // delete cube instance and cube desc
        CubeInstance cube = getCube(cubeName);

        if (deleteDesc && cube.getDescriptor() != null)
            store.deleteResource(cube.getDescriptor().getResourcePath());

        store.deleteResource(cube.getResourcePath());

        // delete cube from project
        ProjectManager.getInstance(config).removeRealizationsFromProjects(RealizationType.CUBE, cubeName);

        // clean cube cache
        this.afterCubeDroped(cube);

        return cube;
    }

    // sync on update
    public CubeInstance createCube(String cubeName, String projectName, CubeDesc desc, String owner) throws IOException {
        logger.info("Creating cube '" + projectName + "-->" + cubeName + "' from desc '" + desc.getName() + "'");

        // save cube resource
        CubeInstance cube = CubeInstance.create(cubeName, projectName, desc);
        cube.setOwner(owner);
        saveResource(cube);

        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.CUBE, cubeName, projectName, owner);

        return cube;
    }

    public CubeInstance updateCube(CubeInstance cube) throws IOException {
        logger.info("Updating cube instance '" + cube.getName());

        // save resource
        saveResource(cube);

        logger.info("Cube with " + cube.getSegments().size() + " segments is saved");

        return cube;
    }

//    public List<CubeSegment> allocateSegments(CubeInstance cubeInstance, CubeBuildTypeEnum buildType, long startDate, long endDate) throws IOException, CubeIntegrityException {
//        if (cubeInstance.getBuildingSegments().size() > 0) {
//            throw new RuntimeException("There is already an allocating segment!");
//        }
//        List<CubeSegment> segments = new ArrayList<CubeSegment>();
//
//        final boolean appendBuildOnHllMeasure = cubeInstance.appendBuildOnHllMeasure(startDate, endDate);
//        if (null != cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateColumn()) {
//            if (appendBuildOnHllMeasure) {
//                long[] dateRange = cubeInstance.getDateRange();
//                segments.add(buildSegment(cubeInstance, dateRange[0], endDate));
//            } else {
//
//                if (startDate == 0 && cubeInstance.getSegments().size() == 0) {
//                    startDate = cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart();
//                }
//
//                // incremental build
//                CubeSegment lastSegment = null;
//                for (CubeSegment segment : cubeInstance.getSegments()) {
//                    if (segment.getDateRangeStart() == startDate) {
//                        // refresh or merge
//                        segments.add(buildSegment(cubeInstance, startDate, endDate));
//                    }
//                    if (segment.getDateRangeStart() < startDate && startDate < segment.getDateRangeEnd()) {
//                        // delete-insert
//                        segments.add(buildSegment(cubeInstance, segment.getDateRangeStart(), startDate));
//                        segments.add(buildSegment(cubeInstance, startDate, endDate));
//                    }
//                    lastSegment = segment;
//                }
//
//                // append
//                if (null == lastSegment || (lastSegment.getDateRangeEnd() == startDate)) {
//                    segments.add(buildSegment(cubeInstance, startDate, endDate));
//                }
//            }
//        } else {
//            segments.add(buildSegment(cubeInstance, 0, 0));
//        }
//
//        validateNewSegments(cubeInstance, buildType, segments.get(0));
//
//        CubeSegment newSeg = segments.get(0);
//        if (buildType == CubeBuildTypeEnum.MERGE) {
//            List<CubeSegment> mergingSegments = cubeInstance.getMergingSegments(newSeg);
//            this.makeDictForNewSegment(cubeInstance, newSeg, mergingSegments);
//            this.makeSnapshotForNewSegment(cubeInstance, newSeg, mergingSegments);
//        } else if (appendBuildOnHllMeasure) {
//            List<CubeSegment> mergingSegments = cubeInstance.getSegment(SegmentStatusEnum.READY);
//            this.makeDictForNewSegment(cubeInstance, newSeg, mergingSegments);
//            this.makeSnapshotForNewSegment(cubeInstance, newSeg, mergingSegments);
//        }
//
//        cubeInstance.getSegments().addAll(segments);
//        Collections.sort(cubeInstance.getSegments());
//
//        this.updateCube(cubeInstance);
//
//        return segments;
//    }

    private boolean hasOverlap(long startDate, long endDate, long anotherStartDate, long anotherEndDate) {
        if (startDate >= endDate) {
            throw new IllegalArgumentException("startDate must be less than endDate");
        }
        if (anotherStartDate >= anotherEndDate) {
            throw new IllegalArgumentException("anotherStartDate must be less than anotherEndDate");
        }
        if (startDate <= anotherStartDate && anotherStartDate < endDate) {
            return true;
        }
        if (startDate < anotherEndDate && anotherEndDate <= endDate) {
            return true;
        }
        return false;
    }

    public CubeSegment mergeSegments(CubeInstance cubeInstance, final long startDate, final long endDate) throws IOException, CubeIntegrityException {
        if (cubeInstance.getBuildingSegments().size() > 0) {
            throw new RuntimeException("There is already an allocating segment!");
        }

        if (cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateColumn() == null) {
            throw new CubeIntegrityException("there is no partition date, only full build is supported");
        }
            List<CubeSegment> readySegments = cubeInstance.getSegment(SegmentStatusEnum.READY);
            if (readySegments.isEmpty()) {
                throw new CubeIntegrityException("there are no segments in ready state");
            }
            long start = Long.MAX_VALUE;
            long end = Long.MIN_VALUE;
            for (CubeSegment readySegment: readySegments) {
                if (hasOverlap(startDate, endDate, readySegment.getDateRangeStart(), readySegment.getDateRangeEnd())) {
                    if (start > readySegment.getDateRangeStart()) {
                        start = readySegment.getDateRangeStart();
                    }
                    if (end < readySegment.getDateRangeEnd()) {
                        end = readySegment.getDateRangeEnd();
                    }
                }
            }
        CubeSegment newSegment = buildSegment(cubeInstance, start, end);

        validateNewSegments(cubeInstance, CubeBuildTypeEnum.MERGE, newSegment);

        List<CubeSegment> mergingSegments = cubeInstance.getMergingSegments(newSegment);
        this.makeDictForNewSegment(cubeInstance, newSegment, mergingSegments);
        this.makeSnapshotForNewSegment(cubeInstance, newSegment, mergingSegments);

        cubeInstance.getSegments().add(newSegment);
        Collections.sort(cubeInstance.getSegments());

        this.updateCube(cubeInstance);

        return newSegment;
    }

    public CubeSegment appendSegments(CubeInstance cubeInstance, long startDate, long endDate) throws IOException, CubeIntegrityException {
        if (cubeInstance.getBuildingSegments().size() > 0) {
            throw new RuntimeException("There is already an allocating segment!");
        }
        List<CubeSegment> readySegments = cubeInstance.getSegments(SegmentStatusEnum.READY);
        CubeSegment newSegment;
        if (cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateColumn() != null) {
            if (readySegments.isEmpty()) {
                newSegment = buildSegment(cubeInstance, cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart(), endDate);
            } else {
                newSegment = buildSegment(cubeInstance, readySegments.get(readySegments.size() - 1).getDateRangeEnd(), endDate);
            }
        } else {
            newSegment = buildSegment(cubeInstance, 0, Long.MAX_VALUE);
        }
        validateNewSegments(cubeInstance, CubeBuildTypeEnum.BUILD, newSegment);

        cubeInstance.getSegments().add(newSegment);
        Collections.sort(cubeInstance.getSegments());
        this.updateCube(cubeInstance);

        return newSegment;
    }

    public static String getHBaseStorageLocationPrefix() {
        return "KYLIN_";
    }

    /**
     * For each cube htable, we leverage htable's metadata to keep track of
     * which kylin server(represented by its kylin_metadata prefix) owns this htable
     */
    public static String getHtableMetadataKey() {
        return "KYLIN_HOST";
    }

    public void updateSegmentOnJobSucceed(CubeInstance cubeInstance, CubeBuildTypeEnum buildType, String segmentName, String jobUuid, long lastBuildTime, long sizeKB, long sourceRecordCount, long sourceRecordsSize) throws IOException, CubeIntegrityException {

        List<CubeSegment> segmentsInNewStatus = cubeInstance.getSegments(SegmentStatusEnum.NEW);
        CubeSegment cubeSegment = cubeInstance.getSegmentById(jobUuid);
        Preconditions.checkArgument(segmentsInNewStatus.size() == 1, "there are " + segmentsInNewStatus.size() + " new segments");

        switch (buildType) {
            case BUILD:
                cubeInstance.getSegments().removeAll(cubeInstance.getMergingSegments());
                break;
            case MERGE:
                cubeInstance.getSegments().removeAll(cubeInstance.getMergingSegments());
                break;
            case REFRESH:
                break;
            default:
                throw new RuntimeException("invalid build type:" + buildType);
        }
        cubeSegment.setLastBuildJobID(jobUuid);
        cubeSegment.setLastBuildTime(lastBuildTime);
        cubeSegment.setSizeKB(sizeKB);
        cubeSegment.setSourceRecords(sourceRecordCount);
        cubeSegment.setSourceRecordsSize(sourceRecordsSize);
        cubeSegment.setStatus(SegmentStatusEnum.READY);
        cubeInstance.setStatus(RealizationStatusEnum.READY);
        this.updateCube(cubeInstance);
    }

    public void updateSegmentOnJobDiscard(CubeInstance cubeInstance, String segmentName) throws IOException, CubeIntegrityException {
        for (int i = 0; i < cubeInstance.getSegments().size(); i++) {
            CubeSegment segment = cubeInstance.getSegments().get(i);
            if (segment.getName().equals(segmentName) && segment.getStatus() != SegmentStatusEnum.READY) {
                cubeInstance.getSegments().remove(segment);
            }
        }
        updateCube(cubeInstance);
    }

    /**
     * After cube update, reload cube related cache
     *
     * @param cube
     */
    public void loadCubeCache(CubeInstance cube) {
        try {
            loadCubeInstance(cube.getResourcePath());
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
    }

    /**
     * After cube deletion, remove cube related cache
     *
     * @param cube
     */
    public void removeCubeCache(CubeInstance cube) {
        cubeMap.remove(cube.getName().toUpperCase());

        for (CubeSegment segment : cube.getSegments()) {
            usedStorageLocation.remove(segment.getName());
        }
    }

    public LookupStringTable getLookupTable(CubeSegment cubeSegment, DimensionDesc dim) {

        String tableName = dim.getTable();
        String[] pkCols = dim.getJoin().getPrimaryKey();
        String key = tableName + "#" + StringUtils.join(pkCols, ",");

        LookupStringTable r = lookupTables.get(key);
        if (r == null) {
            String snapshotResPath = cubeSegment.getSnapshotResPath(tableName);
            if (snapshotResPath == null)
                throw new IllegalStateException("No snaphot for table '" + tableName + "' found on cube segment" + cubeSegment.getCubeInstance().getName() + "/" + cubeSegment);

            try {
                SnapshotTable snapshot = getSnapshotManager().getSnapshotTable(snapshotResPath);
                TableDesc tableDesc = getMetadataManager().getTableDesc(tableName);
                r = new LookupStringTable(tableDesc, pkCols, snapshot);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load lookup table " + tableName + " from snapshot " + snapshotResPath, e);
            }

            lookupTables.putLocal(key, r);
        }

        return r;
    }

    /**
     * For the new segment, we need to create dictionaries for it, too. For
     * those dictionaries on fact table, create it by merging underlying
     * dictionaries For those dictionaries on lookup table, just copy it from
     * any one of the merging segments, it's guaranteed to be consistent(checked
     * in CubeSegmentValidator)
     *
     * @param cube
     * @param newSeg
     * @throws IOException
     */
    private void makeDictForNewSegment(CubeInstance cube, CubeSegment newSeg, List<CubeSegment> mergingSegments) throws IOException {
        HashSet<TblColRef> colsNeedMeringDict = new HashSet<TblColRef>();
        HashSet<TblColRef> colsNeedCopyDict = new HashSet<TblColRef>();
        DictionaryManager dictMgr = this.getDictionaryManager();

        CubeDesc cubeDesc = cube.getDescriptor();
        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            for (TblColRef col : dim.getColumnRefs()) {
                if (newSeg.getCubeDesc().getRowkey().isUseDictionary(col)) {
                    String dictTable = (String) dictMgr.decideSourceData(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, null)[0];
                    if (cubeDesc.getFactTable().equalsIgnoreCase(dictTable)) {
                        colsNeedMeringDict.add(col);
                    } else {
                        colsNeedCopyDict.add(col);
                    }
                }
            }
        }

        for (TblColRef col : colsNeedMeringDict) {
            logger.info("Merging fact table dictionary on : " + col);
            List<DictionaryInfo> dictInfos = new ArrayList<DictionaryInfo>();
            for (CubeSegment segment : mergingSegments) {
                logger.info("Including fact table dictionary of segment : " + segment);
                DictionaryInfo dictInfo = dictMgr.getDictionaryInfo(segment.getDictResPath(col));
                dictInfos.add(dictInfo);
            }
            this.mergeDictionaries(newSeg, dictInfos, col);
        }

        for (TblColRef col : colsNeedCopyDict) {
            String path = mergingSegments.get(0).getDictResPath(col);
            newSeg.putDictResPath(col, path);
        }
    }

    /**
     * make snapshots for the new segment by copying from one of the underlying
     * merging segments. it's ganranteed to be consistent(checked in
     * CubeSegmentValidator)
     *
     * @param cube
     * @param newSeg
     */
    private void makeSnapshotForNewSegment(CubeInstance cube, CubeSegment newSeg, List<CubeSegment> mergingSegments) {
        for (Map.Entry<String, String> entry : mergingSegments.get(0).getSnapshots().entrySet()) {
            newSeg.putSnapshotResPath(entry.getKey(), entry.getValue());
        }
    }

    private DictionaryInfo mergeDictionaries(CubeSegment cubeSeg, List<DictionaryInfo> dicts, TblColRef col) throws IOException {
        DictionaryManager dictMgr = getDictionaryManager();
        DictionaryInfo dictInfo = dictMgr.mergeDictionary(dicts);
        cubeSeg.putDictResPath(col, dictInfo.getResourcePath());

        return dictInfo;
    }

    private void saveResource(CubeInstance cube) throws IOException {
        ResourceStore store = getStore();
        store.putResource(cube.getResourcePath(), cube, CUBE_SERIALIZER);
        this.afterCubeUpdated(cube);
    }

    private void afterCubeUpdated(CubeInstance updatedCube) {
        MetadataManager.getInstance(config).reload();
        cubeMap.put(updatedCube.getName().toUpperCase(), updatedCube);
    }

    private void afterCubeDroped(CubeInstance droppedCube) {
        MetadataManager.getInstance(config).reload();
        removeCubeCache(droppedCube);
    }

    /**
     * @param cubeInstance
     * @param startDate    (pass 0 if full build)
     * @param endDate      (pass 0 if full build)
     * @return
     */
    private CubeSegment buildSegment(CubeInstance cubeInstance, long startDate, long endDate) {
        CubeSegment segment = new CubeSegment();
        String incrementalSegName = CubeSegment.getSegmentName(startDate, endDate);
        segment.setUuid(UUID.randomUUID().toString());
        segment.setName(incrementalSegName);
        segment.setCreateTime(DateStrDictionary.dateToString(new Date()));
        segment.setDateRangeStart(startDate);
        segment.setDateRangeEnd(endDate);
        segment.setStatus(SegmentStatusEnum.NEW);
        segment.setStorageLocationIdentifier(generateStorageLocation());

        segment.setCubeInstance(cubeInstance);

        return segment;
    }

    private String generateStorageLocation() {
        String namePrefix = getHBaseStorageLocationPrefix();
        String tableName = "";
        do {
            StringBuffer sb = new StringBuffer();
            sb.append(namePrefix);
            for (int i = 0; i < HBASE_TABLE_LENGTH; i++) {
                int idx = (int) (Math.random() * ALPHA_NUM.length());
                sb.append(ALPHA_NUM.charAt(idx));
            }
            tableName = sb.toString();
        } while (this.usedStorageLocation.contains(tableName));

        return tableName;
    }

    /**
     */
    private void validateNewSegments(CubeInstance cubeInstance, CubeBuildTypeEnum buildType, CubeSegment newSegment) throws CubeIntegrityException {
        if (null == cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateColumn()) {
            // do nothing for non-incremental build
            return;
        }
        if (newSegment.getDateRangeEnd() <= newSegment.getDateRangeStart()) {
            throw new CubeIntegrityException(" end date.");
        }

        CubeSegmentValidator cubeSegmentValidator = CubeSegmentValidator.getCubeSegmentValidator(buildType);
        cubeSegmentValidator.validate(cubeInstance, newSegment);
    }

    private void loadAllCubeInstance() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.CUBE_RESOURCE_ROOT, ".json");

        logger.debug("Loading Cube from folder " + store.getReadableResourcePath(ResourceStore.CUBE_RESOURCE_ROOT));

        for (String path : paths) {
            loadCubeInstance(path);
        }

        logger.debug("Loaded " + paths.size() + " Cube(s)");
    }

    private synchronized CubeInstance loadCubeInstance(String path) throws IOException {
        ResourceStore store = getStore();

        CubeInstance cubeInstance = null;
        try {
            cubeInstance = store.getResource(path, CubeInstance.class, CUBE_SERIALIZER);
            cubeInstance.setConfig(config);

            if (StringUtils.isBlank(cubeInstance.getName()))
                throw new IllegalStateException("CubeInstance name must not be blank");

            cubeMap.putLocal(cubeInstance.getName().toUpperCase(), cubeInstance);

            for (CubeSegment segment : cubeInstance.getSegments()) {
                usedStorageLocation.add(segment.getName());
            }

            return cubeInstance;
        } catch (Exception e) {
            logger.error("Error during load cube instance " + path, e);
            return null;
        }
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private DictionaryManager getDictionaryManager() {
        return DictionaryManager.getInstance(config);
    }

    private SnapshotManager getSnapshotManager() {
        return SnapshotManager.getInstance(config);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    @Override
    public RealizationType getRealizationType() {
        return RealizationType.CUBE;
    }

    @Override
    public IRealization getRealization(String name) {
        return getCube(name);
    }
}
