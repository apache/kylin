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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DictionaryDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.SourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author yangli9
 */
public class CubeManager implements IRealizationProvider {

    private static String ALPHA_NUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static int HBASE_TABLE_LENGTH = 10;
    public static final Serializer<CubeInstance> CUBE_SERIALIZER = new JsonSerializer<CubeInstance>(CubeInstance.class);

    private static final Logger logger = LoggerFactory.getLogger(CubeManager.class);

    // static cached instances
    private static final ConcurrentMap<KylinConfig, CubeManager> CACHE = new ConcurrentHashMap<KylinConfig, CubeManager>();

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
                        logger.warn("type: " + kylinConfig.getClass() + " reference: "
                                + System.identityHashCode(kylinConfig.base()));
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
    private ConcurrentMap<String, String> usedStorageLocation = new ConcurrentHashMap<>();

    private CubeManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeManager with config " + config);
        this.config = config;
        this.cubeMap = new CaseInsensitiveStringCache<CubeInstance>(config, "cube");

        // touch lower level metadata before registering my listener
        loadAllCubeInstance();
        Broadcaster.getInstance(config).registerListener(new CubeSyncListener(), "cube");
    }

    private class CubeSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real instanceof CubeInstance) {
                    reloadCubeLocal(real.getName());
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            String cubeName = cacheKey;

            if (event == Event.DROP)
                removeCubeLocal(cubeName);
            else
                reloadCubeLocal(cubeName);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjects(RealizationType.CUBE,
                    cubeName)) {
                broadcaster.notifyProjectDataUpdate(prj.getName());
            }
        }
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

    public DictionaryInfo buildDictionary(CubeSegment cubeSeg, TblColRef col, IReadableTable inpTable)
            throws IOException {
        CubeDesc cubeDesc = cubeSeg.getCubeDesc();
        if (!cubeDesc.getAllColumnsNeedDictionaryBuilt().contains(col))
            return null;

        String builderClass = cubeDesc.getDictionaryBuilderClass(col);
        DictionaryInfo dictInfo = getDictionaryManager().buildDictionary(col, inpTable,
                builderClass);

        saveDictionaryInfo(cubeSeg, col, dictInfo);
        return dictInfo;
    }

    public DictionaryInfo saveDictionary(CubeSegment cubeSeg, TblColRef col, IReadableTable inpTable,
            Dictionary<String> dict) throws IOException {
        CubeDesc cubeDesc = cubeSeg.getCubeDesc();
        if (!cubeDesc.getAllColumnsNeedDictionaryBuilt().contains(col))
            return null;

        DictionaryInfo dictInfo = getDictionaryManager().saveDictionary(col, inpTable, dict);

        saveDictionaryInfo(cubeSeg, col, dictInfo);
        return dictInfo;
    }

    private void saveDictionaryInfo(CubeSegment cubeSeg, TblColRef col, DictionaryInfo dictInfo) throws IOException {
        if (dictInfo != null) {
            Dictionary<?> dict = dictInfo.getDictionaryObject();
            cubeSeg.putDictResPath(col, dictInfo.getResourcePath());
            cubeSeg.getRowkeyStats().add(new Object[] { col.getIdentity(), dict.getSize(), dict.getSizeOfId() });

            CubeUpdate update = new CubeUpdate(cubeSeg.getCubeInstance());
            update.setToUpdateSegs(cubeSeg);
            updateCube(update);
        }
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
                throw new IllegalStateException("No dictionary found by " + dictResPath
                        + ", invalid cube state; cube segment" + cubeSeg + ", col " + col);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get dictionary for cube segment" + cubeSeg + ", col" + col, e);
        }
        return (Dictionary<String>) info.getDictionaryObject();
    }

    public SnapshotTable buildSnapshotTable(CubeSegment cubeSeg, String lookupTable) throws IOException {
        MetadataManager metaMgr = getMetadataManager();
        SnapshotManager snapshotMgr = getSnapshotManager();

        TableDesc tableDesc = new TableDesc(metaMgr.getTableDesc(lookupTable, cubeSeg.getProject()));
        IReadableTable hiveTable = SourceFactory.createReadableTable(tableDesc);
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

        // remove cube and update cache
        getStore().deleteResource(cube.getResourcePath());
        cubeMap.remove(cube.getName());
        Cuboid.clearCache(cube);

        if (deleteDesc && cube.getDescriptor() != null) {
            CubeDescManager.getInstance(config).removeCubeDesc(cube.getDescriptor());
        }

        // delete cube from project
        ProjectManager.getInstance(config).removeRealizationsFromProjects(RealizationType.CUBE, cubeName);

        return cube;
    }

    // sync on update
    public CubeInstance createCube(String cubeName, String projectName, CubeDesc desc, String owner)
            throws IOException {
        logger.info("Creating cube '" + projectName + "-->" + cubeName + "' from desc '" + desc.getName() + "'");

        // save cube resource
        CubeInstance cube = CubeInstance.create(cubeName, desc);
        cube.setOwner(owner);

        updateCubeWithRetry(new CubeUpdate(cube), 0);
        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.CUBE, cubeName, projectName, owner);

        return cube;
    }

    public CubeInstance createCube(CubeInstance cube, String projectName, String owner) throws IOException {
        logger.info("Creating cube '" + projectName + "-->" + cube.getName() + "' from instance object. '");

        // save cube resource
        cube.setOwner(owner);

        updateCubeWithRetry(new CubeUpdate(cube), 0);
        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.CUBE, cube.getName(), projectName,
                owner);

        return cube;
    }

    public CubeInstance updateCube(CubeUpdate update) throws IOException {
        CubeInstance cube = updateCubeWithRetry(update, 0);
        return cube;
    }

    private CubeInstance updateCubeWithRetry(CubeUpdate update, int retry) throws IOException {
        if (update == null || update.getCubeInstance() == null)
            throw new IllegalStateException();

        CubeInstance cube = update.getCubeInstance();
        logger.info("Updating cube instance '" + cube.getName() + "'");

        Segments<CubeSegment> newSegs = (Segments) cube.getSegments().clone();

        if (update.getToAddSegs() != null)
            newSegs.addAll(Arrays.asList(update.getToAddSegs()));

        List<String> toRemoveResources = Lists.newArrayList();
        if (update.getToRemoveSegs() != null) {
            Iterator<CubeSegment> iterator = newSegs.iterator();
            while (iterator.hasNext()) {
                CubeSegment currentSeg = iterator.next();
                for (CubeSegment toRemoveSeg : update.getToRemoveSegs()) {
                    if (currentSeg.getUuid().equals(toRemoveSeg.getUuid())) {
                        logger.info("Remove segment " + currentSeg.toString());
                        toRemoveResources.add(currentSeg.getStatisticsResourcePath());
                        iterator.remove();
                        break;
                    }
                }
            }
        }

        if (update.getToUpdateSegs() != null) {
            for (CubeSegment segment : update.getToUpdateSegs()) {
                for (int i = 0; i < newSegs.size(); i++) {
                    if (newSegs.get(i).getUuid().equals(segment.getUuid())) {
                        newSegs.set(i, segment);
                        break;
                    }
                }
            }
        }

        Collections.sort(newSegs);
        newSegs.validate();
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

        if (update.getCuboids() != null) {
            cube.setCuboids(update.getCuboids());
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
        return appendSegment(cube, null, null, null, null);
    }

    public CubeSegment appendSegment(CubeInstance cube, TSRange tsRange) throws IOException {
        return appendSegment(cube, tsRange, null, null, null);
    }

    public CubeSegment appendSegment(CubeInstance cube, SourcePartition src) throws IOException {
        return appendSegment(cube, src.getTSRange(), src.getSegRange(), src.getSourcePartitionOffsetStart(),
                src.getSourcePartitionOffsetEnd());
    }

    CubeSegment appendSegment(CubeInstance cube, TSRange tsRange, SegmentRange segRange,
            Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd)
            throws IOException {
        checkInputRanges(tsRange, segRange);
        checkBuildingSegment(cube);

        // fix start/end a bit
        if (cube.getModel().getPartitionDesc().isPartitioned()) {
            // if missing start, set it to where last time ends
            CubeSegment last = cube.getLastSegment();
            if (last != null && !last.isOffsetCube() && tsRange.start.v == 0) {
                tsRange = new TSRange(last.getTSRange().end.v, tsRange.end.v);
            }
        } else {
            // full build
            tsRange = null;
            segRange = null;
        }

        CubeSegment newSegment = newSegment(cube, tsRange, segRange);
        newSegment.setSourcePartitionOffsetStart(sourcePartitionOffsetStart);
        newSegment.setSourcePartitionOffsetEnd(sourcePartitionOffsetEnd);
        validateNewSegments(cube, newSegment);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);
        updateCube(cubeBuilder);
        return newSegment;
    }

    public CubeSegment refreshSegment(CubeInstance cube, TSRange tsRange, SegmentRange segRange)
            throws IOException {
        checkInputRanges(tsRange, segRange);
        checkBuildingSegment(cube);

        CubeSegment newSegment = newSegment(cube, tsRange, segRange);

        Pair<Boolean, Boolean> pair = cube.getSegments().fitInSegments(newSegment);
        if (pair.getFirst() == false || pair.getSecond() == false)
            throw new IllegalArgumentException("The new refreshing segment " + newSegment
                    + " does not match any existing segment in cube " + cube);

        if (segRange != null) {
            CubeSegment toRefreshSeg = null;
            for (CubeSegment cubeSegment : cube.getSegments()) {
                if (cubeSegment.getSegRange().equals(segRange)) {
                    toRefreshSeg = cubeSegment;
                    break;
                }
            }

            if (toRefreshSeg == null) {
                throw new IllegalArgumentException("For streaming cube, only one segment can be refreshed at one time");
            }

            newSegment.setSourcePartitionOffsetStart(toRefreshSeg.getSourcePartitionOffsetStart());
            newSegment.setSourcePartitionOffsetEnd(toRefreshSeg.getSourcePartitionOffsetEnd());
        }

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);
        updateCube(cubeBuilder);

        return newSegment;
    }

    public CubeSegment mergeSegments(CubeInstance cube, TSRange tsRange, SegmentRange segRange, boolean force)
            throws IOException {
        if (cube.getSegments().isEmpty())
            throw new IllegalArgumentException("Cube " + cube + " has no segments");

        checkInputRanges(tsRange, segRange);
        checkBuildingSegment(cube);
        checkCubeIsPartitioned(cube);

        if (cube.getSegments().getFirstSegment().isOffsetCube()) {
            // offset cube, merge by date range?
            if (segRange == null && tsRange != null) {
                Pair<CubeSegment, CubeSegment> pair = cube.getSegments(SegmentStatusEnum.READY)
                        .findMergeOffsetsByDateRange(tsRange, Long.MAX_VALUE);
                if (pair == null)
                    throw new IllegalArgumentException("Find no segments to merge by " + tsRange + " for cube " + cube);
                segRange = new SegmentRange(pair.getFirst().getSegRange().start, pair.getSecond().getSegRange().end);
            }
            tsRange = null;
            Preconditions.checkArgument(segRange != null);
        } else {
            segRange = null;
            Preconditions.checkArgument(tsRange != null);
        }

        CubeSegment newSegment = newSegment(cube, tsRange, segRange);

        Segments<CubeSegment> mergingSegments = cube.getMergingSegments(newSegment);
        if (mergingSegments.size() <= 1)
            throw new IllegalArgumentException("Range " + newSegment.getSegRange()
                    + " must contain at least 2 segments, but there is " + mergingSegments.size());

        CubeSegment first = mergingSegments.get(0);
        CubeSegment last = mergingSegments.get(mergingSegments.size() - 1);
        if (first.isOffsetCube()) {
            newSegment.setSegRange(new SegmentRange(first.getSegRange().start, last.getSegRange().end));
            newSegment.setSourcePartitionOffsetStart(first.getSourcePartitionOffsetStart());
            newSegment.setSourcePartitionOffsetEnd(last.getSourcePartitionOffsetEnd());
            newSegment.setTSRange(null);
        } else {
            newSegment.setTSRange(new TSRange(mergingSegments.getTSStart(), mergingSegments.getTSEnd()));
            newSegment.setSegRange(null);
        }

        if (force == false) {
            List<String> emptySegment = Lists.newArrayList();
            for (CubeSegment seg : mergingSegments) {
                if (seg.getSizeKB() == 0) {
                    emptySegment.add(seg.getName());
                }
            }

            if (emptySegment.size() > 0) {
                throw new IllegalArgumentException(
                        "Empty cube segment found, couldn't merge unless 'forceMergeEmptySegment' set to true: "
                                + emptySegment);
            }
        }

        validateNewSegments(cube, newSegment);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToAddSegs(newSegment);
        updateCube(cubeBuilder);

        return newSegment;
    }
    
    private void checkInputRanges(TSRange tsRange, SegmentRange segRange) {
        if (tsRange != null && segRange != null) {
            throw new IllegalArgumentException("Build or refresh cube segment either by TSRange or by SegmentRange, not both.");
        }
    }

    private void checkBuildingSegment(CubeInstance cube) {
        int maxBuldingSeg = cube.getConfig().getMaxBuildingSegments();
        if (cube.getBuildingSegments().size() >= maxBuldingSeg) {
            throw new IllegalStateException(
                    "There is already " + cube.getBuildingSegments().size() + " building segment; ");
        }
    }

    private void checkCubeIsPartitioned(CubeInstance cube) {
        if (cube.getDescriptor().getModel().getPartitionDesc().isPartitioned() == false) {
            throw new IllegalStateException(
                    "there is no partition date column specified, only full build is supported");
        }
    }

    /**
     * After cube update, reload cube related cache
     *
     * @param cubeName
     */
    public CubeInstance reloadCubeLocal(String cubeName) {
        CubeInstance cubeInstance = reloadCubeLocalAt(CubeInstance.concatResourcePath(cubeName));
        Cuboid.clearCache(cubeInstance);
        return cubeInstance;
    }

    public void removeCubeLocal(String cubeName) {
        CubeInstance cube = cubeMap.get(cubeName);
        if (cube != null) {
            cubeMap.removeLocal(cubeName);
            for (CubeSegment segment : cube.getSegments()) {
                usedStorageLocation.remove(segment.getUuid());
            }
            Cuboid.clearCache(cube);
        }
    }

    public LookupStringTable getLookupTable(CubeSegment cubeSegment, JoinDesc join) {

        String tableName = join.getPKSide().getTableIdentity();
        String[] pkCols = join.getPrimaryKey();
        String snapshotResPath = cubeSegment.getSnapshotResPath(tableName);
        if (snapshotResPath == null)
            throw new IllegalStateException("No snapshot for table '" + tableName + "' found on cube segment"
                    + cubeSegment.getCubeInstance().getName() + "/" + cubeSegment);

        try {
            SnapshotTable snapshot = getSnapshotManager().getSnapshotTable(snapshotResPath);
            TableDesc tableDesc = getMetadataManager().getTableDesc(tableName, cubeSegment.getProject());
            return new LookupStringTable(tableDesc, pkCols, snapshot);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load lookup table " + tableName + " from snapshot " + snapshotResPath, e);
        }
    }

    private CubeSegment newSegment(CubeInstance cube, TSRange tsRange, SegmentRange segRange) {
        CubeSegment segment = new CubeSegment();
        segment.setUuid(UUID.randomUUID().toString());
        segment.setName(CubeSegment.makeSegmentName(tsRange, segRange));
        segment.setCreateTimeUTC(System.currentTimeMillis());
        segment.setCubeInstance(cube);
        
        // let full build range be backward compatible
        if (tsRange == null && segRange == null)
            tsRange = new TSRange(0L, Long.MAX_VALUE);
        
        segment.setTSRange(tsRange);
        segment.setSegRange(segRange);
        segment.setStatus(SegmentStatusEnum.NEW);
        segment.setStorageLocationIdentifier(generateStorageLocation());

        segment.setCubeInstance(cube);

        segment.validate();
        return segment;
    }

    @VisibleForTesting
    /*private*/ String generateStorageLocation() {
        String namePrefix = config.getHBaseTableNamePrefix();
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

    public void promoteNewlyBuiltSegments(CubeInstance cube, CubeSegment newSegment) throws IOException {
        if (StringUtils.isBlank(newSegment.getStorageLocationIdentifier()))
            throw new IllegalStateException(
                    "For cube " + cube + ", segment " + newSegment + " missing StorageLocationIdentifier");

        if (StringUtils.isBlank(newSegment.getLastBuildJobID()))
            throw new IllegalStateException("For cube " + cube + ", segment " + newSegment + " missing LastBuildJobID");

        if (isReady(newSegment) == true) {
            logger.warn("For cube " + cube + ", segment " + newSegment + " state should be NEW but is READY");
        }

        List<CubeSegment> tobe = cube.calculateToBeSegments(newSegment);

        if (tobe.contains(newSegment) == false)
            throw new IllegalStateException(
                    "For cube " + cube + ", segment " + newSegment + " is expected but not in the tobe " + tobe);

        newSegment.setStatus(SegmentStatusEnum.READY);

        List<CubeSegment> toRemoveSegs = Lists.newArrayList();
        for (CubeSegment segment : cube.getSegments()) {
            if (!tobe.contains(segment))
                toRemoveSegs.add(segment);
        }

        logger.info("Promoting cube " + cube + ", new segment " + newSegment + ", to remove segments " + toRemoveSegs);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToRemoveSegs(toRemoveSegs.toArray(new CubeSegment[toRemoveSegs.size()]))
                .setToUpdateSegs(newSegment).setStatus(RealizationStatusEnum.READY);
        updateCube(cubeBuilder);
    }

    public void validateNewSegments(CubeInstance cube, CubeSegment newSegments) {
        List<CubeSegment> tobe = cube.calculateToBeSegments(newSegments);
        List<CubeSegment> newList = Arrays.asList(newSegments);
        if (tobe.containsAll(newList) == false) {
            throw new IllegalStateException("For cube " + cube + ", the new segments " + newList
                    + " do not fit in its current " + cube.getSegments() + "; the resulted tobe is " + tobe);
        }
    }

    private boolean isReady(CubeSegment seg) {
        return seg.getStatus() == SegmentStatusEnum.READY;
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

    private CubeInstance reloadCubeLocalAt(String path) {
        ResourceStore store = getStore();
        CubeInstance cube;

        try {
            cube = store.getResource(path, CubeInstance.class, CUBE_SERIALIZER);
            checkNotNull(cube, "cube (at %s) not found", path);

            String cubeName = cube.getName();
            checkState(StringUtils.isNotBlank(cubeName), "cube (at %s) name must not be blank", path);

            CubeDesc cubeDesc = CubeDescManager.getInstance(config).getCubeDesc(cube.getDescName());
            checkNotNull(cubeDesc, "cube descriptor '%s' (for cube '%s') not found", cube.getDescName(), cubeName);
            if (!isSpecialTestCube(cubeName))
                checkState(cubeDesc.getName().equals(cubeName),
                        "cube name '%s' must be same as descriptor name '%s', but it is not", cubeName,
                        cubeDesc.getName());

            if (!cubeDesc.getError().isEmpty()) {
                cube.setStatus(RealizationStatusEnum.DESCBROKEN);
                logger.error("cube descriptor {} (for cube '{}') is broken", cubeDesc.getResourcePath(), cubeName);
                for (String error : cubeDesc.getError()) {
                    logger.error("Error: {}", error);
                }
            } else if (cube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
                cube.setStatus(RealizationStatusEnum.DISABLED);
                logger.info("cube {} changed from DESCBROKEN to DISABLED", cubeName);
            }

            cube.setConfig((KylinConfigExt) cubeDesc.getConfig());
            cubeMap.putLocal(cubeName, cube);

            for (CubeSegment segment : cube.getSegments()) {
                usedStorageLocation.put(segment.getUuid(), segment.getStorageLocationIdentifier());
            }

            logger.info("Reloaded cube {} being {} having {} segments", cubeName, cube, cube.getSegments().size());
            return cube;

        } catch (Exception e) {
            logger.error("Error during load cube instance, skipping : " + path, e);
            return null;
        }
    }

    private boolean isSpecialTestCube(String cubeName) {
        return cubeName.equals("kylin_sales_cube") //
                || config.isDevEnv()
                        && (cubeName.startsWith("test_kylin_cube") || cubeName.startsWith("test_streaming"));
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

    /**
     * Calculate the holes (gaps) in segments.
     * @param cubeName
     * @return
     */
    public List<CubeSegment> calculateHoles(String cubeName) {
        List<CubeSegment> holes = Lists.newArrayList();
        final CubeInstance cube = getCube(cubeName);
        Preconditions.checkNotNull(cube);
        final List<CubeSegment> segments = cube.getSegments();
        logger.info("totally " + segments.size() + " cubeSegments");
        if (segments.size() == 0) {
            return holes;
        }

        Collections.sort(segments);
        for (int i = 0; i < segments.size() - 1; ++i) {
            CubeSegment first = segments.get(i);
            CubeSegment second = segments.get(i + 1);
            if (first.getSegRange().connects(second.getSegRange()))
                continue;
            
            if (first.getSegRange().apartBefore(second.getSegRange())) {
                CubeSegment hole = new CubeSegment();
                hole.setCubeInstance(cube);
                if (first.isOffsetCube()) {
                    hole.setSegRange(new SegmentRange(first.getSegRange().end, second.getSegRange().start));
                    hole.setSourcePartitionOffsetStart(first.getSourcePartitionOffsetEnd());
                    hole.setSourcePartitionOffsetEnd(second.getSourcePartitionOffsetStart());
                    hole.setName(CubeSegment.makeSegmentName(null, hole.getSegRange()));
                } else {
                    hole.setTSRange(new TSRange(first.getTSRange().end.v, second.getTSRange().start.v));
                    hole.setName(CubeSegment.makeSegmentName(hole.getTSRange(), null));
                }
                holes.add(hole);
            }
        }
        return holes;
    }

    private final String GLOBAL_DICTIONNARY_CLASS = "org.apache.kylin.dict.GlobalDictionaryBuilder";

    //UHC (ultra high cardinality column): contain the ShardByColumns and the GlobalDictionaryColumns
    public int[] getUHCIndex(CubeDesc cubeDesc) throws IOException {
        List<TblColRef> dictCols = Lists.newArrayList(cubeDesc.getAllColumnsNeedDictionaryBuilt());
        int[] uhcIndex = new int[dictCols.size()];

        //add GlobalDictionaryColumns
        List<DictionaryDesc> dictionaryDescList = cubeDesc.getDictionaries();
        if (dictionaryDescList != null) {
            for (DictionaryDesc dictionaryDesc : dictionaryDescList) {
                if (dictionaryDesc.getBuilderClass() != null
                        && dictionaryDesc.getBuilderClass().equalsIgnoreCase(GLOBAL_DICTIONNARY_CLASS)) {
                    for (int i = 0; i < dictCols.size(); i++) {
                        if (dictCols.get(i).equals(dictionaryDesc.getColumnRef())) {
                            uhcIndex[i] = 1;
                            break;
                        }
                    }
                }
            }
        }

        //add ShardByColumns
        Set<TblColRef> shardByColumns = cubeDesc.getShardByColumns();
        for (int i = 0; i < dictCols.size(); i++) {
            if (shardByColumns.contains(dictCols.get(i))) {
                uhcIndex[i] = 1;
            }
        }

        return uhcIndex;
    }
}
