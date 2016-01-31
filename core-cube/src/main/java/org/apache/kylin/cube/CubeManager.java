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

package org.apache.kylin.cube;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * @author yangli9
 */
public class CubeManager implements IRealizationProvider {

    private static String ALPHA_NUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static int HBASE_TABLE_LENGTH = 10;
    public static final Serializer<CubeInstance> CUBE_SERIALIZER = new JsonSerializer<CubeInstance>(CubeInstance.class);

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

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    // cube name ==> CubeInstance
    private CaseInsensitiveStringCache<CubeInstance> cubeMap;
    // "table/column" ==> lookup table
    //    private SingleValueCache<String, LookupStringTable> lookupTables = new SingleValueCache<String, LookupStringTable>(Broadcaster.TYPE.METADATA);

    // for generation hbase table name of a new segment
    private Multimap<String, String> usedStorageLocation = HashMultimap.create();

    private CubeManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeManager with config " + config);
        this.config = config;
        this.cubeMap = new CaseInsensitiveStringCache<CubeInstance>(config, Broadcaster.TYPE.CUBE);
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
            if (descName.equalsIgnoreCase(ci.getDescName())) {
                result.add(ci);
            }
        }
        return result;
    }

    public DictionaryInfo buildDictionary(CubeSegment cubeSeg, TblColRef col, DistinctColumnValuesProvider factTableValueProvider) throws IOException {
        CubeDesc cubeDesc = cubeSeg.getCubeDesc();
        if (!cubeDesc.getAllColumnsNeedDictionary().contains(col))
            return null;

        DictionaryManager dictMgr = getDictionaryManager();
        DictionaryInfo dictInfo = dictMgr.buildDictionary(cubeDesc.getModel(),true, col, factTableValueProvider);

        if (dictInfo != null) {
            cubeSeg.putDictResPath(col, dictInfo.getResourcePath());

            CubeUpdate cubeBuilder = new CubeUpdate(cubeSeg.getCubeInstance());
            cubeBuilder.setToUpdateSegs(cubeSeg);
            updateCube(cubeBuilder);
        }
        return dictInfo;
    }

    /**
     * return null if no dictionary for given column
     */
    @SuppressWarnings("unchecked")
    public Dictionary<String> getDictionary(CubeSegment cubeSeg, TblColRef col) {
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

        return (Dictionary<String>) info.getDictionaryObject();
    }

    public SnapshotTable buildSnapshotTable(CubeSegment cubeSeg, String lookupTable) throws IOException {
        MetadataManager metaMgr = getMetadataManager();
        SnapshotManager snapshotMgr = getSnapshotManager();

        TableDesc tableDesc = metaMgr.getTableDesc(lookupTable);
        ReadableTable hiveTable = SourceFactory.createReadableTable(tableDesc);
        SnapshotTable snapshot = snapshotMgr.buildSnapshot(hiveTable, tableDesc);

        cubeSeg.putSnapshotResPath(lookupTable, snapshot.getResourcePath());
        CubeUpdate cubeBuilder = new CubeUpdate(cubeSeg.getCubeInstance());
        cubeBuilder.setToUpdateSegs(cubeSeg);
        updateCube(cubeBuilder);

        return snapshot;
    }

    // sync on update
    public CubeInstance dropCube(String cubeName, boolean deleteDesc) throws IOException {
        logger.info("Dropping cube '" + cubeName + "'");
        // load projects before remove cube from project

        // delete cube instance and cube desc
        CubeInstance cube = getCube(cubeName);

        if (deleteDesc && cube.getDescriptor() != null) {
            CubeDescManager.getInstance(config).removeCubeDesc(cube.getDescriptor());
        }

        // remove cube and update cache
        getStore().deleteResource(cube.getResourcePath());
        cubeMap.remove(cube.getName());

        // delete cube from project
        ProjectManager.getInstance(config).removeRealizationsFromProjects(RealizationType.CUBE, cubeName);

        if (listener != null)
            listener.afterCubeDelete(cube);

        return cube;
    }

    // sync on update
    public CubeInstance createCube(String cubeName, String projectName, CubeDesc desc, String owner) throws IOException {
        logger.info("Creating cube '" + projectName + "-->" + cubeName + "' from desc '" + desc.getName() + "'");

        // save cube resource
        CubeInstance cube = CubeInstance.create(cubeName, desc);
        cube.setOwner(owner);

        updateCubeWithRetry(new CubeUpdate(cube), 0);
        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.CUBE, cubeName, projectName, owner);

        if (listener != null)
            listener.afterCubeCreate(cube);

        return cube;
    }

    public CubeInstance createCube(CubeInstance cube, String projectName, String owner) throws IOException {
        logger.info("Creating cube '" + projectName + "-->" + cube.getName() + "' from instance object. '");

        // save cube resource
        cube.setOwner(owner);

        updateCubeWithRetry(new CubeUpdate(cube), 0);
        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.CUBE, cube.getName(), projectName, owner);

        if (listener != null)
            listener.afterCubeCreate(cube);

        return cube;
    }

    public CubeInstance updateCube(CubeUpdate update) throws IOException {
        CubeInstance cube = updateCubeWithRetry(update, 0);

        if (listener != null)
            listener.afterCubeUpdate(cube);

        return cube;
    }

    private boolean validateReadySegments(CubeInstance cube) {
        final List<CubeSegment> readySegments = cube.getSegments(SegmentStatusEnum.READY);
        if (readySegments.size() == 0) {
            return true;
        }
        for (CubeSegment readySegment : readySegments) {
            if (readySegment.getDateRangeEnd() <= readySegment.getDateRangeStart()) {
                logger.warn(String.format("segment:%s has invalid date range:[%d, %d], validation failed", readySegment.getName(), readySegment.getDateRangeStart(), readySegment.getDateRangeEnd()));
                return false;
            }
        }
        Collections.sort(readySegments);
        for (int i = 0, size = readySegments.size(); i < size - 1; i++) {
            CubeSegment lastSegment = readySegments.get(i);
            CubeSegment segment = readySegments.get(i + 1);
            if (lastSegment.getDateRangeEnd() <= segment.getDateRangeStart()) {
                continue;
            } else {
                logger.warn(String.format("segment:%s and %s data range has overlap, validation failed", lastSegment.getName(), segment.getName()));
                return false;
            }
        }
        return true;
    }

    private CubeInstance updateCubeWithRetry(CubeUpdate update, int retry) throws IOException {
        if (update == null || update.getCubeInstance() == null)
            throw new IllegalStateException();

        CubeInstance cube = update.getCubeInstance();
        logger.info("Updating cube instance '" + cube.getName() + "'");

        if (update.getToAddSegs() != null)
            cube.getSegments().addAll(Arrays.asList(update.getToAddSegs()));

        List<String> toRemoveResources = Lists.newArrayList();
        if (update.getToRemoveSegs() != null) {
            Iterator<CubeSegment> iterator = cube.getSegments().iterator();
            while (iterator.hasNext()) {
                CubeSegment currentSeg = iterator.next();
                for (CubeSegment toRemoveSeg : update.getToRemoveSegs()) {
                    if (currentSeg.getUuid().equals(toRemoveSeg.getUuid())) {
                        iterator.remove();
                        toRemoveResources.add(toRemoveSeg.getStatisticsResourcePath());
                    }
                }
            }

        }

        if (update.getToUpdateSegs() != null) {
            for (CubeSegment segment : update.getToUpdateSegs()) {
                for (int i = 0; i < cube.getSegments().size(); i++) {
                    if (cube.getSegments().get(i).getUuid().equals(segment.getUuid())) {
                        cube.getSegments().set(i, segment);
                    }
                }
            }
        }

        Collections.sort(cube.getSegments());

        if (!validateReadySegments(cube)) {
            throw new IllegalStateException("Has invalid Ready segments in cube " + cube.getName());
        }

        if (update.getStatus() != null) {
            cube.setStatus(update.getStatus());
        }

        if (update.getOwner() != null) {
            cube.setOwner(update.getOwner());
        }

        if (update.getCost() > 0) {
            cube.setCost(update.getCost());
        }

        try {
            getStore().putResource(cube.getResourcePath(), cube, CUBE_SERIALIZER);
        } catch (IllegalStateException ise) {
            logger.warn("Write conflict to update cube " + cube.getName() + " at try " + retry + ", will retry...");
            if (retry >= 7) {
                logger.error("Retried 7 times till got error, abandoning...", ise);
                throw ise;
            }

            cube = reloadCubeLocal(cube.getName());
            update.setCubeInstance(cube);
            retry++;
            cube = updateCubeWithRetry(update, retry);
        }

        if (toRemoveResources.size() > 0) {
            for (String resource : toRemoveResources) {
                try {
                    getStore().deleteResource(resource);
                } catch (IOException ioe) {
                    logger.error("Failed to delete resource " + toRemoveResources.toString());
                }
            }
        }

        cubeMap.put(cube.getName(), cube);

        //this is a duplicate call to take care of scenarios where REST cache service unavailable
        ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).clearL2Cache();

        return cube;
    }

    public Pair<CubeSegment, CubeSegment> appendAndMergeSegments(CubeInstance cube, long endDate) throws IOException {
        checkNoBuildingSegment(cube);
        checkCubeIsPartitioned(cube);

        if (cube.getSegments().size() == 0)
            throw new IllegalStateException("expect at least one existing segment");

        long appendStart = calculateStartDateForAppendSegment(cube);
        CubeSegment appendSegment = newSegment(cube, appendStart, endDate);

        long startDate = cube.getDescriptor().getPartitionDateStart();
        CubeSegment mergeSegment = newSegment(cube, startDate, endDate);

        validateNewSegments(cube, mergeSegment);

        CubeUpdate cubeBuilder = new CubeUpdate(cube).setToAddSegs(appendSegment, mergeSegment);
        updateCube(cubeBuilder);

        return Pair.newPair(appendSegment, mergeSegment);
    }

    public CubeSegment appendSegments(CubeInstance cube, long endDate) throws IOException {
        return appendSegments(cube, endDate, true, true);
    }

    public CubeSegment appendSegments(CubeInstance cube, long endDate, boolean strictChecking, boolean saveChange) throws IOException {
        long startDate = 0;
        if (cube.getDescriptor().getModel().getPartitionDesc().isPartitioned()) {
            startDate = calculateStartDateForAppendSegment(cube);
            if (endDate > cube.getDescriptor().getPartitionDateEnd()) {
                SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                f.setTimeZone(TimeZone.getTimeZone("GMT"));
                throw new IllegalArgumentException("The selected date couldn't be later than cube's end date '" + f.format(new Date(cube.getDescriptor().getPartitionDateEnd())) + "'.");
            }
        } else {
            endDate = Long.MAX_VALUE;
        }
        return appendSegments(cube, startDate, endDate, strictChecking, saveChange);
    }

    public CubeSegment appendSegments(CubeInstance cube, long startDate, long endDate, boolean strictChecking, boolean saveChange) throws IOException {
        if (strictChecking)
            checkNoBuildingSegment(cube);

        CubeSegment newSegment = newSegment(cube, startDate, endDate);
        validateNewSegments(cube, strictChecking, newSegment);

        if (saveChange) {

            CubeUpdate cubeBuilder = new CubeUpdate(cube);
            cubeBuilder.setToAddSegs(newSegment);
            updateCube(cubeBuilder);
        }
        return newSegment;
    }

    public CubeSegment refreshSegment(CubeInstance cube, long startDate, long endDate) throws IOException {
        checkNoBuildingSegment(cube);

        CubeSegment newSegment = newSegment(cube, startDate, endDate);
        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);
        updateCube(cubeBuilder);

        return newSegment;
    }

    public CubeSegment mergeSegments(CubeInstance cube, final long startDate, final long endDate, boolean forceMergeEmptySeg) throws IOException {
        checkNoBuildingSegment(cube);
        checkCubeIsPartitioned(cube);

        Pair<Long, Long> range = alignMergeRange(cube, startDate, endDate);
        CubeSegment newSegment = newSegment(cube, range.getFirst(), range.getSecond());

        List<CubeSegment> mergingSegments = cube.getMergingSegments(newSegment);

        if (forceMergeEmptySeg == false) {
            List<String> emptySegment = Lists.newArrayList();
            for (CubeSegment seg : mergingSegments) {
                if (seg.getSizeKB() == 0) {
                    emptySegment.add(seg.getName());
                }
            }

            if (emptySegment.size() > 0) {
                throw new IllegalArgumentException("Empty cube segment found, couldn't merge unless 'forceMergeEmptySegment' set to true: " + emptySegment);
            }
        }

        validateNewSegments(cube, false, newSegment);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);
        updateCube(cubeBuilder);

        return newSegment;
    }

    private Pair<Long, Long> alignMergeRange(CubeInstance cube, long startDate, long endDate) {
        List<CubeSegment> readySegments = cube.getSegments(SegmentStatusEnum.READY);
        if (readySegments.isEmpty()) {
            throw new IllegalStateException("there are no segments in ready state");
        }
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        for (CubeSegment readySegment : readySegments) {
            if (hasOverlap(startDate, endDate, readySegment.getDateRangeStart(), readySegment.getDateRangeEnd())) {
                if (start > readySegment.getDateRangeStart()) {
                    start = readySegment.getDateRangeStart();
                }
                if (end < readySegment.getDateRangeEnd()) {
                    end = readySegment.getDateRangeEnd();
                }
            }
        }
        return Pair.newPair(start, end);
    }

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

    private long calculateStartDateForAppendSegment(CubeInstance cube) {
        List<CubeSegment> existing = cube.getSegments();
        if (existing.isEmpty()) {
            return cube.getDescriptor().getPartitionDateStart();
        } else {
            return existing.get(existing.size() - 1).getDateRangeEnd();
        }
    }

    private void checkNoBuildingSegment(CubeInstance cube) {
        if (cube.getBuildingSegments().size() > 0) {
            throw new IllegalStateException("There is already a building segment!");
        }
    }

    private void checkCubeIsPartitioned(CubeInstance cube) {
        if (cube.getDescriptor().getModel().getPartitionDesc().isPartitioned() == false) {
            throw new IllegalStateException("there is no partition date column specified, only full build is supported");
        }
    }

    /**
     * After cube update, reload cube related cache
     *
     * @param cubeName
     */
    public CubeInstance reloadCubeLocal(String cubeName) {
        return reloadCubeLocalAt(CubeInstance.concatResourcePath(cubeName));
    }

    public void removeCubeLocal(String cubeName) {
        usedStorageLocation.removeAll(cubeName.toUpperCase());
        cubeMap.removeLocal(cubeName);
    }

    public LookupStringTable getLookupTable(CubeSegment cubeSegment, DimensionDesc dim) {

        String tableName = dim.getTable();
        String[] pkCols = dim.getJoin().getPrimaryKey();
        String snapshotResPath = cubeSegment.getSnapshotResPath(tableName);
        if (snapshotResPath == null)
            throw new IllegalStateException("No snaphot for table '" + tableName + "' found on cube segment" + cubeSegment.getCubeInstance().getName() + "/" + cubeSegment);

        try {
            SnapshotTable snapshot = getSnapshotManager().getSnapshotTable(snapshotResPath);
            TableDesc tableDesc = getMetadataManager().getTableDesc(tableName);
            return new LookupStringTable(tableDesc, pkCols, snapshot);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load lookup table " + tableName + " from snapshot " + snapshotResPath, e);
        }
    }

    private CubeSegment newSegment(CubeInstance cubeInstance, long startDate, long endDate) {
        if (startDate >= endDate)
            throw new IllegalArgumentException("New segment range invalid, start date must be earlier than end date, " + startDate + " < " + endDate);

        CubeSegment segment = new CubeSegment();
        String incrementalSegName = CubeSegment.getSegmentName(startDate, endDate);
        segment.setUuid(UUID.randomUUID().toString());
        segment.setName(incrementalSegName);
        Date creatTime = new Date();
        segment.setCreateTimeUTC(creatTime.getTime());
        segment.setDateRangeStart(startDate);
        segment.setDateRangeEnd(endDate);
        segment.setStatus(SegmentStatusEnum.NEW);
        segment.setStorageLocationIdentifier(generateStorageLocation());

        segment.setCubeInstance(cubeInstance);

        segment.validate();
        return segment;
    }

    private String generateStorageLocation() {
        String namePrefix = IRealizationConstants.CubeHbaseStorageLocationPrefix;
        String tableName = "";
        Random ran = new Random();
        do {
            StringBuffer sb = new StringBuffer();
            sb.append(namePrefix);
            for (int i = 0; i < HBASE_TABLE_LENGTH; i++) {
                sb.append(ALPHA_NUM.charAt(ran.nextInt(ALPHA_NUM.length())));
            }
            tableName = sb.toString();
        } while (this.usedStorageLocation.containsValue(tableName));

        return tableName;
    }

    public CubeSegment autoMergeCubeSegments(CubeInstance cube) throws IOException {
        if (!cube.needAutoMerge()) {
            logger.debug("Cube " + cube.getName() + " doesn't need auto merge");
            return null;
        }

        if (cube.getBuildingSegments().size() > 0) {
            logger.debug("Cube " + cube.getName() + " has bulding segment, will not trigger merge at this moment");
            return null;
        }

        List<CubeSegment> readySegments = Lists.newArrayList(cube.getSegments(SegmentStatusEnum.READY));

        if (readySegments.size() == 0) {
            logger.debug("Cube " + cube.getName() + " has no ready segment to merge");
            return null;
        }

        long[] timeRanges = cube.getDescriptor().getAutoMergeTimeRanges();
        Arrays.sort(timeRanges);

        CubeSegment newSeg = null;
        for (int i = timeRanges.length - 1; i >= 0; i--) {
            long toMergeRange = timeRanges[i];
            long currentRange = 0;
            long lastEndTime = 0;
            List<CubeSegment> toMergeSegments = Lists.newArrayList();
            for (CubeSegment segment : readySegments) {
                long thisSegmentRange = segment.getDateRangeEnd() - segment.getDateRangeStart();

                if (thisSegmentRange >= toMergeRange) {
                    // this segment and its previous segments will not be merged
                    toMergeSegments.clear();
                    currentRange = 0;
                    lastEndTime = segment.getDateRangeEnd();
                    continue;
                }

                if (segment.getDateRangeStart() != lastEndTime && toMergeSegments.isEmpty() == false) {
                    // gap exists, give up the small segments before the gap;
                    toMergeSegments.clear();
                    currentRange = 0;
                }

                currentRange += thisSegmentRange;
                if (currentRange < toMergeRange) {
                    toMergeSegments.add(segment);
                    lastEndTime = segment.getDateRangeEnd();
                } else {
                    // merge
                    toMergeSegments.add(segment);

                    newSeg = newSegment(cube, toMergeSegments.get(0).getDateRangeStart(), segment.getDateRangeEnd());
                    // only one merge job be created here
                    return newSeg;
                }
            }

        }

        return null;
    }

    public void promoteNewlyBuiltSegments(CubeInstance cube, CubeSegment... newSegments) throws IOException {
        List<CubeSegment> tobe = calculateToBeSegments(cube, false);

        for (CubeSegment seg : newSegments) {
            if (tobe.contains(seg) == false)
                throw new IllegalStateException("For cube " + cube + ", segment " + seg + " is expected but not in the tobe " + tobe);

            if (StringUtils.isBlank(seg.getStorageLocationIdentifier()))
                throw new IllegalStateException("For cube " + cube + ", segment " + seg + " missing StorageLocationIdentifier");

            if (StringUtils.isBlank(seg.getLastBuildJobID()))
                throw new IllegalStateException("For cube " + cube + ", segment " + seg + " missing LastBuildJobID");

            seg.setStatus(SegmentStatusEnum.READY);
        }

        for (CubeSegment seg : tobe) {
            if (isReady(seg) == false)
                throw new IllegalStateException("For cube " + cube + ", segment " + seg + " should be READY but is not");
        }

        List<CubeSegment> toRemoveSegs = Lists.newArrayList();
        for (CubeSegment segment : cube.getSegments()) {
            if (!tobe.contains(segment))
                toRemoveSegs.add(segment);
        }

        logger.info("Promoting cube " + cube + ", new segments " + Arrays.toString(newSegments) + ", to remove segments " + toRemoveSegs);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToRemoveSegs(toRemoveSegs.toArray(new CubeSegment[toRemoveSegs.size()])).setToUpdateSegs(newSegments).setStatus(RealizationStatusEnum.READY);
        updateCube(cubeBuilder);
    }

    public void validateNewSegments(CubeInstance cube, CubeSegment... newSegments) {
        validateNewSegments(cube, true, newSegments);
    }

    public void validateNewSegments(CubeInstance cube, boolean strictChecking, CubeSegment... newSegments) {
        List<CubeSegment> tobe = calculateToBeSegments(cube, strictChecking, newSegments);
        List<CubeSegment> newList = Arrays.asList(newSegments);
        if (tobe.containsAll(newList) == false) {
            throw new IllegalStateException("For cube " + cube + ", the new segments " + newList + " do not fit in its current " + cube.getSegments() + "; the resulted tobe is " + tobe);
        }
    }

    /**
     * Smartly figure out the TOBE segments once all new segments are built.
     * - Ensures no gap, no overlap
     * - Favors new segments over the old
     * - Favors big segments over the small
     */
    private List<CubeSegment> calculateToBeSegments(CubeInstance cube, boolean strictChecking, CubeSegment... newSegments) {

        List<CubeSegment> tobe = Lists.newArrayList(cube.getSegments());
        if (newSegments != null)
            tobe.addAll(Arrays.asList(newSegments));
        if (tobe.size() == 0)
            return tobe;

        // sort by start time, then end time
        Collections.sort(tobe);

        // check first segment start time
        CubeSegment firstSeg = tobe.get(0);
        firstSeg.validate();

        for (int i = 0, j = 1; j < tobe.size();) {
            CubeSegment is = tobe.get(i);
            CubeSegment js = tobe.get(j);
            js.validate();

            // check i is either ready or new
            if (!isNew(is) && !isReady(is)) {
                tobe.remove(i);
                continue;
            }

            // check j is either ready or new
            if (!isNew(js) && !isReady(js)) {
                tobe.remove(j);
                continue;
            }

            // if i, j competes
            if (is.getDateRangeStart() == js.getDateRangeStart()) {
                // if both new or ready, favor the bigger segment
                if (isReady(is) && isReady(js) || isNew(is) && isNew(js)) {
                    if (is.getDateRangeEnd() <= js.getDateRangeEnd()) {
                        tobe.remove(i);
                    } else {
                        tobe.remove(j);
                    }
                }
                // otherwise, favor the new segment
                else if (isNew(is)) {
                    tobe.remove(j);
                } else {
                    tobe.remove(i);
                }
                continue;
            }

            // if i, j in sequence
            if ((!strictChecking && is.getDateRangeEnd() <= js.getDateRangeStart() || strictChecking && is.getDateRangeEnd() == js.getDateRangeStart())) {
                i++;
                j++;
                continue;
            }

            // seems j not fitting
            tobe.remove(j);
        }

        return tobe;
    }

    private boolean isReady(CubeSegment seg) {
        return seg.getStatus() == SegmentStatusEnum.READY;
    }

    private boolean isNew(CubeSegment seg) {
        return seg.getStatus() == SegmentStatusEnum.NEW || seg.getStatus() == SegmentStatusEnum.READY_PENDING;
    }

    private void loadAllCubeInstance() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.CUBE_RESOURCE_ROOT, ".json");

        logger.debug("Loading Cube from folder " + store.getReadableResourcePath(ResourceStore.CUBE_RESOURCE_ROOT));

        for (String path : paths) {
            reloadCubeLocalAt(path);
        }

        logger.debug("Loaded " + paths.size() + " Cube(s)");
    }

    private synchronized CubeInstance reloadCubeLocalAt(String path) {
        ResourceStore store = getStore();

        CubeInstance cubeInstance;
        try {
            cubeInstance = store.getResource(path, CubeInstance.class, CUBE_SERIALIZER);
            cubeInstance.setConfig(config);

            if (StringUtils.isBlank(cubeInstance.getName()))
                throw new IllegalStateException("CubeInstance name must not be blank, at " + path);

            if (cubeInstance.getDescriptor() == null)
                throw new IllegalStateException("CubeInstance desc not found '" + cubeInstance.getDescName() + "', at " + path);

            final String cubeName = cubeInstance.getName();
            cubeMap.putLocal(cubeName, cubeInstance);

            for (CubeSegment segment : cubeInstance.getSegments()) {
                usedStorageLocation.put(cubeName.toUpperCase(), segment.getStorageLocationIdentifier());
            }

            logger.debug("Reloaded new cube: " + cubeName + " with reference being" + cubeInstance + " having " + cubeInstance.getSegments().size() + " segments:" + StringUtils.join(Collections2.transform(cubeInstance.getSegments(), new Function<CubeSegment, String>() {
                @Nullable
                @Override
                public String apply(CubeSegment input) {
                    return input.getStorageLocationIdentifier();
                }
            }), ","));

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

    // ============================================================================

    public interface CubeChangeListener {
        void afterCubeCreate(CubeInstance cube);

        void afterCubeUpdate(CubeInstance cube);

        void afterCubeDelete(CubeInstance cube);
    }

    private CubeChangeListener listener;

    public void setCubeChangeListener(CubeChangeListener listener) {
        this.listener = listener;
    }

    /**
     * Get the columns which need build the dictionary from fact table. (the column exists on fact and is not fk)
     * @param cubeDesc
     * @return
     * @throws IOException
     */
    public List<TblColRef> getAllDictColumnsOnFact(CubeDesc cubeDesc) throws IOException {
        List<TblColRef> dictionaryColumns = cubeDesc.getAllColumnsNeedDictionary();

        List<TblColRef> factDictCols = new ArrayList<TblColRef>();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        for (int i = 0; i < dictionaryColumns.size(); i++) {
            TblColRef col = dictionaryColumns.get(i);

            String scanTable = dictMgr.decideSourceData(cubeDesc.getModel(), true, col).getTable();
            if (cubeDesc.getModel().isFactTable(scanTable)) {
                factDictCols.add(col);
            }
        }
        return factDictCols;
    }
}
