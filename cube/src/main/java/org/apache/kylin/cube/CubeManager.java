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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.HiveTable;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


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
    private CaseInsensitiveStringCache<CubeInstance> cubeMap = new CaseInsensitiveStringCache<CubeInstance>(Broadcaster.TYPE.CUBE);
    // "table/column" ==> lookup table
    //    private SingleValueCache<String, LookupStringTable> lookupTables = new SingleValueCache<String, LookupStringTable>(Broadcaster.TYPE.METADATA);

    // for generation hbase table name of a new segment
    private Multimap<String, String> usedStorageLocation = HashMultimap.create();

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
        this.afterCubeDropped(cube);

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
        saveResource(cube);
        return cube;
    }

    public Pair<CubeSegment, CubeSegment> appendAndMergeSegments(CubeInstance cube, long endDate) throws IOException {
        checkNoBuildingSegment(cube);
        checkCubeIsPartitioned(cube);

        if (cube.getSegments().size() == 0)
            throw new IllegalStateException("expect at least one existing segment");

        long appendStart = calculateStartDateForAppendSegment(cube);
        CubeSegment appendSegment = newSegment(cube, appendStart, endDate);

        long startDate = cube.getDescriptor().getModel().getPartitionDesc().getPartitionDateStart();
        CubeSegment mergeSegment = newSegment(cube, startDate, endDate);

        validateNewSegments(cube, mergeSegment);
        cube.getSegments().add(appendSegment);
        cube.getSegments().add(mergeSegment);
        Collections.sort(cube.getSegments());
        updateCube(cube);

        return new Pair<CubeSegment, CubeSegment>(appendSegment, mergeSegment);
    }

    public CubeSegment appendSegments(CubeInstance cube, long endDate) throws IOException {
        checkNoBuildingSegment(cube);

        CubeSegment newSegment;
        if (cube.getDescriptor().getModel().getPartitionDesc().isPartitioned()) {
            long startDate = calculateStartDateForAppendSegment(cube);
            newSegment = newSegment(cube, startDate, endDate);
        } else {
            newSegment = newSegment(cube, 0, Long.MAX_VALUE);
        }

        validateNewSegments(cube, newSegment);
        cube.getSegments().add(newSegment);
        Collections.sort(cube.getSegments());
        updateCube(cube);

        return newSegment;
    }


    public CubeSegment refreshSegment(CubeInstance cube, long startDate, long endDate) throws IOException {
        checkNoBuildingSegment(cube);

        CubeSegment newSegment = newSegment(cube, startDate, endDate);
        cube.getSegments().add(newSegment);
        Collections.sort(cube.getSegments());
        updateCube(cube);

        return newSegment;
    }

    public CubeSegment mergeSegments(CubeInstance cube, final long startDate, final long endDate) throws IOException {
        checkNoBuildingSegment(cube);
        checkCubeIsPartitioned(cube);

        Pair<Long, Long> range = alignMergeRange(cube, startDate, endDate);
        CubeSegment newSegment = newSegment(cube, range.getFirst(), range.getSecond());

        validateNewSegments(cube, newSegment);
        cube.getSegments().add(newSegment);
        Collections.sort(cube.getSegments());
        updateCube(cube);

        return newSegment;
    }

    private Pair<Long, Long> alignMergeRange(CubeInstance cube, long startDate, long endDate) {
        List<CubeSegment> readySegments = cube.getSegment(SegmentStatusEnum.READY);
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
        return new Pair<Long, Long>(start, end);
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
            return cube.getDescriptor().getModel().getPartitionDesc().getPartitionDateStart();
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

    public void updateSegmentOnJobDiscard(CubeInstance cubeInstance, String segmentName) throws IOException {
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
     * @param cubeName
     */
    public void loadCubeCache(String cubeName) {
        try {
            loadCubeInstance(CubeInstance.concatResourcePath(cubeName));
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
        final String cubeName = cube.getName().toUpperCase();
        cubeMap.remove(cubeName);
        usedStorageLocation.removeAll(cubeName);
    }

    public void removeCubeCacheLocal(String cubeName) {
        cubeMap.removeLocal(cubeName);
        usedStorageLocation.removeAll(cubeName);
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

    private void saveResource(CubeInstance cube) throws IOException {
        ResourceStore store = getStore();
        store.putResource(cube.getResourcePath(), cube, CUBE_SERIALIZER);
        this.afterCubeUpdated(cube);
    }

    private void afterCubeUpdated(CubeInstance updatedCube) {
        cubeMap.put(updatedCube.getName(), updatedCube);
    }

    private void afterCubeDropped(CubeInstance droppedCube) {
        removeCubeCache(droppedCube);
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
        do {
            StringBuffer sb = new StringBuffer();
            sb.append(namePrefix);
            Random ran = new Random();
            for (int i = 0; i < HBASE_TABLE_LENGTH; i++) {
                sb.append(ALPHA_NUM.charAt(ran.nextInt(ALPHA_NUM.length())));
            }
            tableName = sb.toString();
        } while (this.usedStorageLocation.containsValue(tableName));

        return tableName;
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

        cube.setSegments(tobe);
        cube.setStatus(RealizationStatusEnum.READY);

        logger.info("Promoting cube " + cube + ", new segments " + newSegments);
        saveResource(cube);
    }

    private void validateNewSegments(CubeInstance cube, CubeSegment... newSegments) {
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
        CubeDesc cubeDesc = cube.getDescriptor();
        PartitionDesc partDesc = cubeDesc.getModel().getPartitionDesc();

        List<CubeSegment> tobe = Lists.newArrayList(cube.getSegments());
        if (newSegments != null)
            tobe.addAll(Arrays.asList(newSegments));
        if (tobe.size() == 0)
            return tobe;

        // sort by start time, then end time
        Collections.sort(tobe);

        // check first segment start time
        CubeSegment firstSeg = tobe.get(0);
        if (firstSeg.getDateRangeStart() != partDesc.getPartitionDateStart()) {
            throw new IllegalStateException("For " + cube + ", the first segment, " + firstSeg + ", must start at " + partDesc.getPartitionDateStart());
        }
        firstSeg.validate();

        for (int i = 0, j = 1; j < tobe.size(); ) {
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
            if (is.getDateRangeEnd() == js.getDateRangeStart()) {
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
            loadCubeInstance(path);
        }

        logger.debug("Loaded " + paths.size() + " Cube(s)");
    }

    private synchronized CubeInstance loadCubeInstance(String path) throws IOException {
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
                usedStorageLocation.put(cubeName, segment.getStorageLocationIdentifier());
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
