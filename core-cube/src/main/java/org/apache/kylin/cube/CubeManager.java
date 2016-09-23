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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
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
                    logger.warn("More than one singleton exist");
                    for (KylinConfig kylinConfig : CACHE.keySet()) {
                        logger.warn("type: " + kylinConfig.getClass() + " reference: " + System.identityHashCode(kylinConfig.base()));
                    }
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

    public CubeInstance getCubeByUuid(String uuid) {
        Collection<CubeInstance> copy = new ArrayList<CubeInstance>(cubeMap.values());
        for (CubeInstance cube : copy) {
            if (uuid.equals(cube.getUuid()))
                return cube;
        }
        return null;
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
        if (!cubeDesc.getAllColumnsNeedDictionaryBuilt().contains(col))
            return null;

        DictionaryManager dictMgr = getDictionaryManager();
        String builderClass = cubeDesc.getDictionaryBuilderClass(col);
        DictionaryInfo dictInfo = dictMgr.buildDictionary(cubeDesc.getModel(), col, factTableValueProvider, builderClass);

        if (dictInfo != null) {
            Dictionary<?> dict = dictInfo.getDictionaryObject();
            cubeSeg.putDictResPath(col, dictInfo.getResourcePath());
            cubeSeg.getRowkeyStats().add(new Object[] { col.getName(), dict.getSize(), dict.getSizeOfId() });

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

        TableDesc tableDesc = new TableDesc(metaMgr.getTableDesc(lookupTable));
        if (TableDesc.TABLE_TYPE_VIRTUAL_VIEW.equalsIgnoreCase(tableDesc.getTableType())) {
            String tableName = tableDesc.getMaterializedName();
            tableDesc.setDatabase(config.getHiveDatabaseForIntermediateTable());
            tableDesc.setName(tableName);
        }

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

    private CubeInstance updateCubeWithRetry(CubeUpdate update, int retry) throws IOException {
        if (update == null || update.getCubeInstance() == null)
            throw new IllegalStateException();

        CubeInstance cube = update.getCubeInstance();
        logger.info("Updating cube instance '" + cube.getName() + "'");

        List<CubeSegment> newSegs = Lists.newArrayList(cube.getSegments());

        if (update.getToAddSegs() != null)
            newSegs.addAll(Arrays.asList(update.getToAddSegs()));

        List<String> toRemoveResources = Lists.newArrayList();
        if (update.getToRemoveSegs() != null) {
            Iterator<CubeSegment> iterator = newSegs.iterator();
            while (iterator.hasNext()) {
                CubeSegment currentSeg = iterator.next();
                boolean found = false;
                for (CubeSegment toRemoveSeg : update.getToRemoveSegs()) {
                    if (currentSeg.getUuid().equals(toRemoveSeg.getUuid())) {
                        iterator.remove();
                        toRemoveResources.add(toRemoveSeg.getStatisticsResourcePath());
                        found = true;
                    }
                }
                if (found == false) {
                    logger.error("Segment '" + currentSeg.getName() + "' doesn't exist for remove.");
                }
            }

        }

        if (update.getToUpdateSegs() != null) {
            for (CubeSegment segment : update.getToUpdateSegs()) {
                boolean found = false;
                for (int i = 0; i < newSegs.size(); i++) {
                    if (newSegs.get(i).getUuid().equals(segment.getUuid())) {
                        newSegs.set(i, segment);
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    logger.error("Segment '" + segment.getName() + "' doesn't exist for update.");
                }
            }
        }

        Collections.sort(newSegs);
        CubeValidator.validate(newSegs);
        cube.setSegments(newSegs);

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
        ProjectManager.getInstance(cube.getConfig()).clearL2Cache();

        return cube;
    }

    // append a full build segment
    public CubeSegment appendSegment(CubeInstance cube) throws IOException {
        return appendSegment(cube, 0, 0, 0, 0);
    }

    public CubeSegment appendSegment(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset) throws IOException {
        return appendSegment(cube, startDate, endDate, startOffset, endOffset, true);
    }

    public CubeSegment appendSegment(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset, boolean strictChecking) throws IOException {

        if (strictChecking)
            checkNoBuildingSegment(cube);

        if (cube.getDescriptor().getModel().getPartitionDesc().isPartitioned()) {
            // try figure out a reasonable start if missing
            if (startDate == 0 && startOffset == 0) {
                boolean isOffsetsOn = endOffset != 0;
                if (isOffsetsOn) {
                    startOffset = calculateStartOffsetForAppendSegment(cube);
                    if (startOffset == Long.MAX_VALUE) {
                        throw new IllegalStateException("There is already one pending for building segment, please submit request later.");
                    }
                } else {
                    startDate = calculateStartDateForAppendSegment(cube);
                }
            }
        } else {
            startDate = 0;
            endDate = Long.MAX_VALUE;
            startOffset = 0;
            endOffset = 0;
        }

        CubeSegment newSegment = newSegment(cube, startDate, endDate, startOffset, endOffset);
        validateNewSegments(cube, newSegment);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);
        updateCube(cubeBuilder);
        return newSegment;
    }

    public CubeSegment refreshSegment(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset) throws IOException {
        checkNoBuildingSegment(cube);

        CubeSegment newSegment = newSegment(cube, startDate, endDate, startOffset, endOffset);

        Pair<Boolean, Boolean> pair = CubeValidator.fitInSegments(cube.getSegments(), newSegment);
        if (pair.getFirst() == false || pair.getSecond() == false)
            throw new IllegalArgumentException("The new refreshing segment " + newSegment + " does not match any existing segment in cube " + cube);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);
        updateCube(cubeBuilder);

        return newSegment;
    }

    public CubeSegment mergeSegments(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset, boolean force) throws IOException {
        if (cube.getSegments().isEmpty())
            throw new IllegalArgumentException("Cube " + cube + " has no segments");
        if (startDate >= endDate && startOffset >= endOffset)
            throw new IllegalArgumentException("Invalid merge range");

        checkNoBuildingSegment(cube);
        checkCubeIsPartitioned(cube);

        boolean isOffsetsOn = cube.getSegments().get(0).isSourceOffsetsOn();

        if (isOffsetsOn) {
            // offset cube, merge by date range?
            if (startOffset == endOffset) {
                Pair<CubeSegment, CubeSegment> pair = findMergeOffsetsByDateRange(cube.getSegments(SegmentStatusEnum.READY), startDate, endDate, Long.MAX_VALUE);
                if (pair == null)
                    throw new IllegalArgumentException("Find no segments to merge by date range " + startDate + "-" + endDate + " for cube " + cube);
                startOffset = pair.getFirst().getSourceOffsetStart();
                endOffset = pair.getSecond().getSourceOffsetEnd();
            }
            startDate = 0;
            endDate = 0;
        } else {
            // date range cube, make sure range is on dates
            if (startDate == endDate) {
                startDate = startOffset;
                endDate = endOffset;
            }
            startOffset = 0;
            endOffset = 0;
        }

        CubeSegment newSegment = newSegment(cube, startDate, endDate, startOffset, endOffset);

        List<CubeSegment> mergingSegments = cube.getMergingSegments(newSegment);
        if (mergingSegments.size() <= 1)
            throw new IllegalArgumentException("Range " + newSegment.getSourceOffsetStart() + "-" + newSegment.getSourceOffsetEnd() + " must contain at least 2 segments, but there is " + mergingSegments.size());

        CubeSegment first = mergingSegments.get(0);
        CubeSegment last = mergingSegments.get(mergingSegments.size() - 1);
        if (newSegment.isSourceOffsetsOn()) {
            newSegment.setDateRangeStart(minDateRangeStart(mergingSegments));
            newSegment.setDateRangeEnd(maxDateRangeEnd(mergingSegments));
            newSegment.setSourceOffsetStart(first.getSourceOffsetStart());
            newSegment.setSourceOffsetEnd(last.getSourceOffsetEnd());
        } else {
            newSegment.setDateRangeStart(first.getSourceOffsetStart());
            newSegment.setDateRangeEnd(last.getSourceOffsetEnd());
        }

        if (force == false) {
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

        validateNewSegments(cube, newSegment);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);
        updateCube(cubeBuilder);

        return newSegment;
    }

    private Pair<CubeSegment, CubeSegment> findMergeOffsetsByDateRange(List<CubeSegment> segments, long startDate, long endDate, long skipSegDateRangeCap) {
        // must be offset cube
        LinkedList<CubeSegment> result = Lists.newLinkedList();
        for (CubeSegment seg : segments) {

            // include if date range overlaps
            if (startDate < seg.getDateRangeEnd() && seg.getDateRangeStart() < endDate) {

                // reject too big segment
                if (seg.getDateRangeEnd() - seg.getDateRangeStart() > skipSegDateRangeCap)
                    break;

                // reject holes
                if (result.size() > 0 && result.getLast().getSourceOffsetEnd() != seg.getSourceOffsetStart())
                    break;

                result.add(seg);
            }
        }

        if (result.size() <= 1)
            return null;
        else
            return Pair.newPair(result.getFirst(), result.getLast());
    }

    private long minDateRangeStart(List<CubeSegment> mergingSegments) {
        long min = Long.MAX_VALUE;
        for (CubeSegment seg : mergingSegments)
            min = Math.min(min, seg.getDateRangeStart());
        return min;
    }

    private long maxDateRangeEnd(List<CubeSegment> mergingSegments) {
        long max = Long.MIN_VALUE;
        for (CubeSegment seg : mergingSegments)
            max = Math.max(max, seg.getDateRangeEnd());
        return max;
    }


    private long calculateStartOffsetForAppendSegment(CubeInstance cube) {
        List<CubeSegment> existing = cube.getSegments();
        if (existing.isEmpty()) {
            return 0;
        } else {
            return existing.get(existing.size() - 1).getSourceOffsetEnd();
        }
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

    private CubeSegment newSegment(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset) {
        CubeSegment segment = new CubeSegment();
        segment.setUuid(UUID.randomUUID().toString());
        segment.setName(CubeSegment.makeSegmentName(startDate, endDate, startOffset, endOffset));
        segment.setCreateTimeUTC(System.currentTimeMillis());
        segment.setDateRangeStart(startDate);
        segment.setDateRangeEnd(endDate);
        segment.setSourceOffsetStart(startOffset);
        segment.setSourceOffsetEnd(endOffset);
        segment.setStatus(SegmentStatusEnum.NEW);
        segment.setStorageLocationIdentifier(generateStorageLocation());

        segment.setCubeInstance(cube);

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

    public Pair<Long, Long> autoMergeCubeSegments(CubeInstance cube) throws IOException {
        if (!cube.needAutoMerge()) {
            logger.debug("Cube " + cube.getName() + " doesn't need auto merge");
            return null;
        }

        if (cube.getBuildingSegments().size() > 0) {
            logger.debug("Cube " + cube.getName() + " has bulding segment, will not trigger merge at this moment");
            return null;
        }

        List<CubeSegment> ready = cube.getSegments(SegmentStatusEnum.READY);

        long[] timeRanges = cube.getDescriptor().getAutoMergeTimeRanges();
        Arrays.sort(timeRanges);

        for (int i = timeRanges.length - 1; i >= 0; i--) {
            long toMergeRange = timeRanges[i];

            for (int s = 0; s < ready.size(); s++) {
                CubeSegment seg = ready.get(s);
                Pair<CubeSegment, CubeSegment> p = findMergeOffsetsByDateRange(ready.subList(s, ready.size()), //
                        seg.getDateRangeStart(), seg.getDateRangeStart() + toMergeRange, toMergeRange);
                if (p != null && p.getSecond().getDateRangeEnd() - p.getFirst().getDateRangeStart() >= toMergeRange)
                    return Pair.newPair(p.getFirst().getSourceOffsetStart(), p.getSecond().getSourceOffsetEnd());
            }
        }

        return null;
    }

    public void promoteNewlyBuiltSegments(CubeInstance cube, CubeSegment... newSegments) throws IOException {
        List<CubeSegment> tobe = calculateToBeSegments(cube);

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
        List<CubeSegment> tobe = calculateToBeSegments(cube, newSegments);
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
    private List<CubeSegment> calculateToBeSegments(CubeInstance cube, CubeSegment... newSegments) {

        List<CubeSegment> tobe = Lists.newArrayList(cube.getSegments());
        if (newSegments != null)
            tobe.addAll(Arrays.asList(newSegments));
        if (tobe.size() == 0)
            return tobe;

        // sort by source offset
        Collections.sort(tobe);

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

            if (is.getSourceOffsetStart() == js.getSourceOffsetStart()) {
                // if i, j competes
                if (isReady(is) && isReady(js) || isNew(is) && isNew(js)) {
                    // if both new or ready, favor the bigger segment
                    if (is.getSourceOffsetEnd() <= js.getSourceOffsetEnd()) {
                        tobe.remove(i);
                    } else {
                        tobe.remove(j);
                    }
                } else if (isNew(is)) {
                    // otherwise, favor the new segment
                    tobe.remove(j);
                } else {
                    tobe.remove(i);
                }
                continue;
            }

            // if i, j in sequence
            if (is.getSourceOffsetEnd() <= js.getSourceOffsetStart()) {
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

        logger.info("Loading Cube from folder " + store.getReadableResourcePath(ResourceStore.CUBE_RESOURCE_ROOT));

        int succeed = 0;
        int fail = 0;
        for (String path : paths) {
            CubeInstance cube = reloadCubeLocalAt(path);
            if (cube == null) {
                fail++;
            } else {
                succeed++;
            }
        }

        logger.info("Loaded " + succeed + " cubes, fail on " + fail + " cubes");
    }

    private synchronized CubeInstance reloadCubeLocalAt(String path) {
        ResourceStore store = getStore();

        CubeInstance cubeInstance;
        try {
            cubeInstance = store.getResource(path, CubeInstance.class, CUBE_SERIALIZER);

            CubeDesc cubeDesc = CubeDescManager.getInstance(config).getCubeDesc(cubeInstance.getDescName());
            if (cubeDesc == null)
                throw new IllegalStateException("CubeInstance desc not found '" + cubeInstance.getDescName() + "', at " + path);

            cubeInstance.setConfig((KylinConfigExt) cubeDesc.getConfig());

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
            logger.error("Error during load cube instance, skipping : " + path, e);
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
        List<TblColRef> factDictCols = new ArrayList<TblColRef>();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        for (TblColRef col : cubeDesc.getAllColumnsNeedDictionaryBuilt()) {

            String scanTable = dictMgr.decideSourceData(cubeDesc.getModel(), col).getTable();
            if (cubeDesc.getModel().isFactTable(scanTable)) {
                factDictCols.add(col);
            }
        }
        return factDictCols;
    }
}
