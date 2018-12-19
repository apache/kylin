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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SnapshotTableDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfoManager;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.LookupProviderFactory;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.DataModelDesc;
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
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.source.SourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author yangli9
 */
public class CubeManager implements IRealizationProvider {

    private static String ALPHA_NUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static int HBASE_TABLE_LENGTH = 10;
    public static final Serializer<CubeInstance> CUBE_SERIALIZER = new JsonSerializer<>(CubeInstance.class);

    private static final Logger logger = LoggerFactory.getLogger(CubeManager.class);

    public static CubeManager getInstance(KylinConfig config) {
        return config.getManager(CubeManager.class);
    }

    // called by reflection
    static CubeManager newInstance(KylinConfig config) throws IOException {
        return new CubeManager(config);
    }

    // ============================================================================

    private KylinConfig config;

    // cube name ==> CubeInstance
    private CaseInsensitiveStringCache<CubeInstance> cubeMap;
    private CachedCrudAssist<CubeInstance> crud;

    // protects concurrent operations around the cached map, to avoid for example
    // writing an entity in the middle of reloading it (dirty read)
    private AutoReadWriteLock cubeMapLock = new AutoReadWriteLock();

    // for generation hbase table name of a new segment
    private ConcurrentMap<String, String> usedStorageLocation = new ConcurrentHashMap<>();

    // a few inner classes to group related methods
    private SegmentAssist segAssist = new SegmentAssist();
    private DictionaryAssist dictAssist = new DictionaryAssist();

    private Random ran = new Random();

    private CubeManager(KylinConfig cfg) throws IOException {
        logger.info("Initializing CubeManager with config {}", cfg);
        this.config = cfg;
        this.cubeMap = new CaseInsensitiveStringCache<CubeInstance>(config, "cube");
        this.crud = new CachedCrudAssist<CubeInstance>(getStore(), ResourceStore.CUBE_RESOURCE_ROOT, CubeInstance.class,
                cubeMap) {
            @Override
            protected CubeInstance initEntityAfterReload(CubeInstance cube, String resourceName) {
                cube.init(config);

                for (CubeSegment segment : cube.getSegments()) {
                    usedStorageLocation.put(segment.getUuid(), segment.getStorageLocationIdentifier());
                }
                return cube;
            }
        };
        this.crud.setCheckCopyOnWrite(true);

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new CubeSyncListener(), "cube");
    }

    private class CubeSyncListener extends Broadcaster.Listener {

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            ProjectManager projectManager = ProjectManager.getInstance(config);
            for (IRealization real : projectManager.listAllRealizations(project)) {
                if (real instanceof CubeInstance) {
                    reloadCubeQuietly(real.getName());
                }
            }
            projectManager.reloadProjectL2Cache(project);
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            String cubeName = cacheKey;

            if (event == Event.DROP)
                removeCubeLocal(cubeName);
            else
                reloadCubeQuietly(cubeName);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjects(RealizationType.CUBE,
                    cubeName)) {
                broadcaster.notifyProjectDataUpdate(prj.getName());
            }
        }
    }

    public List<CubeInstance> listAllCubes() {
        try (AutoLock lock = cubeMapLock.lockForRead()) {
            return new ArrayList<CubeInstance>(cubeMap.values());
        }
    }

    public CubeInstance getCube(String cubeName) {
        try (AutoLock lock = cubeMapLock.lockForRead()) {
            return cubeMap.get(cubeName);
        }
    }

    public CubeInstance getCubeByUuid(String uuid) {
        try (AutoLock lock = cubeMapLock.lockForRead()) {
            for (CubeInstance cube : cubeMap.values()) {
                if (uuid.equals(cube.getUuid()))
                    return cube;
            }
            return null;
        }
    }

    /**
     * Get related Cubes by cubedesc name. By default, the desc name will be
     * translated into upper case.
     *
     * @param descName CubeDesc name
     * @return
     */
    public List<CubeInstance> getCubesByDesc(String descName) {
        try (AutoLock lock = cubeMapLock.lockForRead()) {
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
    }

    public CubeInstance createCube(String cubeName, String projectName, CubeDesc desc, String owner)
            throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            logger.info("Creating cube '{}-->{}' from desc '{}'", projectName, cubeName, desc.getName());

            // save cube resource
            CubeInstance cube = CubeInstance.create(cubeName, desc);
            cube.setOwner(owner);
            updateCubeWithRetry(new CubeUpdate(cube), 0);

            ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.CUBE, cubeName, projectName,
                    owner);

            return cube;
        }
    }

    public CubeInstance createCube(CubeInstance cube, String projectName, String owner) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            logger.info("Creating cube '{}-->{}' from instance object. '", projectName, cube.getName());

            // save cube resource
            cube.setOwner(owner);
            updateCubeWithRetry(new CubeUpdate(cube), 0);

            ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.CUBE, cube.getName(),
                    projectName, owner);

            return cube;
        }
    }

    /**
     * when clear all segments, it's supposed to reinitialize the CubeInstance
     */
    public CubeInstance clearSegments(CubeInstance cube) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            cube = cube.latestCopyForWrite(); // get a latest copy
            CubeUpdate update = new CubeUpdate(cube);
            update.setToRemoveSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
            update.setCuboids(Maps.<Long, Long> newHashMap());
            update.setCuboidsRecommend(Sets.<Long> newHashSet());
            update.setUpdateTableSnapshotPath(Maps.<String, String> newHashMap());
            update.setCreateTimeUTC(System.currentTimeMillis());
            update.setCuboidLastOptimized(0L);
            return updateCube(update);
        }
    }

    // try minimize the use of this method, use udpateCubeXXX() instead
    public CubeInstance updateCube(CubeUpdate update) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            CubeInstance cube = updateCubeWithRetry(update, 0);
            return cube;
        }
    }

    public CubeInstance updateCubeStatus(CubeInstance cube, RealizationStatusEnum newStatus) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            cube = cube.latestCopyForWrite(); // get a latest copy
            CubeUpdate update = new CubeUpdate(cube);
            update.setStatus(newStatus);
            ProjectManager.getInstance(config).touchProject(cube.getProject());
            return updateCube(update);
        }
    }

    public CubeInstance updateCubeDropSegments(CubeInstance cube, Collection<CubeSegment> segsToDrop)
            throws IOException {
        CubeSegment[] arr = (CubeSegment[]) segsToDrop.toArray(new CubeSegment[segsToDrop.size()]);
        return updateCubeDropSegments(cube, arr);
    }

    public CubeInstance updateCubeDropSegments(CubeInstance cube, CubeSegment... segsToDrop) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            cube = cube.latestCopyForWrite(); // get a latest copy
            CubeUpdate update = new CubeUpdate(cube);
            update.setToRemoveSegs(segsToDrop);
            return updateCube(update);
        }
    }

    public CubeInstance updateCubeSegStatus(CubeSegment seg, SegmentStatusEnum status) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            CubeInstance cube = seg.getCubeInstance().latestCopyForWrite();
            seg = cube.getSegmentById(seg.getUuid());

            CubeUpdate update = new CubeUpdate(cube);
            seg.setStatus(status);
            update.setToUpdateSegs(seg);
            return updateCube(update);
        }
    }

    public CubeInstance updateCubeLookupSnapshot(CubeInstance cube, String lookupTableName, String newSnapshotResPath) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            cube = cube.latestCopyForWrite();

            CubeUpdate update = new CubeUpdate(cube);
            Map<String, String> map = Maps.newHashMap();
            map.put(lookupTableName, newSnapshotResPath);
            update.setUpdateTableSnapshotPath(map);
            return updateCube(update);
        }
    }

    private CubeInstance updateCubeWithRetry(CubeUpdate update, int retry) throws IOException {
        if (update == null || update.getCubeInstance() == null)
            throw new IllegalStateException();

        CubeInstance cube = update.getCubeInstance();
        logger.info("Updating cube instance '{}'", cube.getName());

        Segments<CubeSegment> newSegs = (Segments) cube.getSegments().clone();

        if (update.getToAddSegs() != null)
            newSegs.addAll(Arrays.asList(update.getToAddSegs()));

        List<String> toRemoveResources = Lists.newArrayList();
        if (update.getToRemoveSegs() != null) {
            processToRemoveSegments(update, newSegs, toRemoveResources);
        }

        if (update.getToUpdateSegs() != null) {
            processToUpdateSegments(update, newSegs);
        }

        Collections.sort(newSegs);
        newSegs.validate();
        cube.setSegments(newSegs);

        setCubeMember(cube, update);

        try {
            cube = crud.save(cube);
        } catch (WriteConflictException ise) {
            logger.warn("Write conflict to update cube {} at try {}, will retry...", cube.getName(), retry);
            if (retry >= 7) {
                logger.error("Retried 7 times till got error, abandoning...", ise);
                throw ise;
            }

            cube = crud.reload(cube.getName());
            update.setCubeInstance(cube.latestCopyForWrite());
            return updateCubeWithRetry(update, ++retry);
        }

        for (String resource : toRemoveResources) {
            try {
                getStore().deleteResource(resource);
            } catch (IOException ioe) {
                logger.error("Failed to delete resource {}", toRemoveResources);
            }
        }

        //this is a duplicate call to take care of scenarios where REST cache service unavailable
        ProjectManager.getInstance(cube.getConfig()).clearL2Cache(cube.getProject());

        return cube;
    }

    private void setCubeMember(CubeInstance cube, CubeUpdate update) {
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

        if (update.getCuboidsRecommend() != null) {
            cube.setCuboidsRecommend(update.getCuboidsRecommend());
        }

        if (update.getUpdateTableSnapshotPath() != null) {
            for(Map.Entry<String, String> lookupSnapshotPathEntry : update.getUpdateTableSnapshotPath().entrySet()) {
                cube.putSnapshotResPath(lookupSnapshotPathEntry.getKey(), lookupSnapshotPathEntry.getValue());
            }
        }

        if (update.getCreateTimeUTC() >= 0) {
            cube.setCreateTimeUTC(update.getCreateTimeUTC());
        }

        if (update.getCuboidLastOptimized() >= 0) {
            cube.setCuboidLastOptimized(update.getCuboidLastOptimized());
        }
    }

    private void processToUpdateSegments(CubeUpdate update, Segments<CubeSegment> newSegs) {
        for (CubeSegment segment : update.getToUpdateSegs()) {
            for (int i = 0; i < newSegs.size(); i++) {
                if (newSegs.get(i).getUuid().equals(segment.getUuid())) {
                    newSegs.set(i, segment);
                    break;
                }
            }
        }
    }

    private void processToRemoveSegments(CubeUpdate update, Segments<CubeSegment> newSegs, List<String> toRemoveResources) {
        Iterator<CubeSegment> iterator = newSegs.iterator();
        while (iterator.hasNext()) {
            CubeSegment currentSeg = iterator.next();
            for (CubeSegment toRemoveSeg : update.getToRemoveSegs()) {
                if (currentSeg.getUuid().equals(toRemoveSeg.getUuid())) {
                    logger.info("Remove segment {}", currentSeg);
                    toRemoveResources.add(currentSeg.getStatisticsResourcePath());
                    iterator.remove();
                    break;
                }
            }
        }
    }

    // for test
    CubeInstance reloadCube(String cubeName) {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            return crud.reload(cubeName);
        }
    }

    public CubeInstance reloadCubeQuietly(String cubeName) {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            CubeInstance cube = crud.reloadQuietly(cubeName);
            if (cube != null)
                Cuboid.clearCache(cube);
            return cube;
        }
    }

    public void removeCubeLocal(String cubeName) {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            CubeInstance cube = cubeMap.get(cubeName);
            if (cube != null) {
                cubeMap.removeLocal(cubeName);
                for (CubeSegment segment : cube.getSegments()) {
                    usedStorageLocation.remove(segment.getUuid());
                }
                Cuboid.clearCache(cube);
            }
        }
    }

    public CubeInstance dropCube(String cubeName, boolean deleteDesc) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            logger.info("Dropping cube '{}'", cubeName);
            // load projects before remove cube from project

            // delete cube instance and cube desc
            CubeInstance cube = getCube(cubeName);

            // remove cube and update cache
            crud.delete(cube);
            Cuboid.clearCache(cube);

            if (deleteDesc && cube.getDescriptor() != null) {
                CubeDescManager.getInstance(config).removeCubeDesc(cube.getDescriptor());
            }

            // delete cube from project
            ProjectManager.getInstance(config).removeRealizationsFromProjects(RealizationType.CUBE, cubeName);

            return cube;
        }
    }

    public ILookupTable getLookupTable(CubeSegment cubeSegment, JoinDesc join) {
        String tableName = join.getPKSide().getTableIdentity();
        CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        SnapshotTableDesc snapshotTableDesc = cubeDesc.getSnapshotTableDesc(tableName);
        if (snapshotTableDesc == null || !snapshotTableDesc.isExtSnapshotTable()) {
            return getInMemLookupTable(cubeSegment, join, snapshotTableDesc);
        } else {
            return getExtLookupTable(cubeSegment, tableName, snapshotTableDesc);
        }
    }

    private ILookupTable getInMemLookupTable(CubeSegment cubeSegment, JoinDesc join, SnapshotTableDesc snapshotTableDesc) {
        String tableName = join.getPKSide().getTableIdentity();
        String snapshotResPath = getSnapshotResPath(cubeSegment, tableName, snapshotTableDesc);
        String[] pkCols = join.getPrimaryKey();

        try {
            SnapshotTable snapshot = getSnapshotManager().getSnapshotTable(snapshotResPath);
            TableDesc tableDesc = getMetadataManager().getTableDesc(tableName, cubeSegment.getProject());
            return LookupProviderFactory.getInMemLookupTable(tableDesc, pkCols, snapshot);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load lookup table " + tableName + " from snapshot " + snapshotResPath, e);
        }
    }

    private ILookupTable getExtLookupTable(CubeSegment cubeSegment, String tableName, SnapshotTableDesc snapshotTableDesc) {
        String snapshotResPath = getSnapshotResPath(cubeSegment, tableName, snapshotTableDesc);

        ExtTableSnapshotInfo extTableSnapshot = ExtTableSnapshotInfoManager.getInstance(config).getSnapshot(
                snapshotResPath);
        TableDesc tableDesc = getMetadataManager().getTableDesc(tableName, cubeSegment.getProject());
        return LookupProviderFactory.getExtLookupTable(tableDesc, extTableSnapshot);
    }

    private String getSnapshotResPath(CubeSegment cubeSegment, String tableName, SnapshotTableDesc snapshotTableDesc) {
        String snapshotResPath;
        if (snapshotTableDesc == null || !snapshotTableDesc.isGlobal()) {
            snapshotResPath = cubeSegment.getSnapshotResPath(tableName);
        } else {
            snapshotResPath = cubeSegment.getCubeInstance().getSnapshotResPath(tableName);
        }
        if (snapshotResPath == null) {
            throw new IllegalStateException("No snapshot for table '" + tableName + "' found on cube segment"
                    + cubeSegment.getCubeInstance().getName() + "/" + cubeSegment);
        }
        return snapshotResPath;
    }

    @VisibleForTesting
    /*private*/ String generateStorageLocation() {
        String namePrefix = config.getHBaseTableNamePrefix();
        String namespace = config.getHBaseStorageNameSpace();
        String tableName = "";
        do {
            StringBuffer sb = new StringBuffer();
            if ((namespace.equals("default") || namespace.equals("")) == false) {
                sb.append(namespace).append(":");
            }
            sb.append(namePrefix);
            for (int i = 0; i < HBASE_TABLE_LENGTH; i++) {
                sb.append(ALPHA_NUM.charAt(ran.nextInt(ALPHA_NUM.length())));
            }
            tableName = sb.toString();
        } while (this.usedStorageLocation.containsValue(tableName));
        return tableName;
    }

    public CubeInstance copyForWrite(CubeInstance cube) {
        return crud.copyForWrite(cube);
    }

    private boolean isReady(CubeSegment seg) {
        return seg.getStatus() == SegmentStatusEnum.READY;
    }

    private TableMetadataManager getTableManager() {
        return TableMetadataManager.getInstance(config);
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
    // Segment related methods
    // ============================================================================

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
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            return segAssist.appendSegment(cube, tsRange, segRange, sourcePartitionOffsetStart,
                    sourcePartitionOffsetEnd);
        }
    }

    public CubeSegment refreshSegment(CubeInstance cube, TSRange tsRange, SegmentRange segRange) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            return segAssist.refreshSegment(cube, tsRange, segRange);
        }
    }

    public CubeSegment[] optimizeSegments(CubeInstance cube, Set<Long> cuboidsRecommend) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            return segAssist.optimizeSegments(cube, cuboidsRecommend);
        }
    }

    public CubeSegment mergeSegments(CubeInstance cube, TSRange tsRange, SegmentRange segRange, boolean force)
            throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            return segAssist.mergeSegments(cube, tsRange, segRange, force);
        }
    }

    public void promoteNewlyBuiltSegments(CubeInstance cube, CubeSegment newSegment) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            segAssist.promoteNewlyBuiltSegments(cube, newSegment);
        }
    }

    public void promoteNewlyOptimizeSegments(CubeInstance cube, CubeSegment... optimizedSegments) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            segAssist.promoteNewlyOptimizeSegments(cube, optimizedSegments);
        }
    }

    public void promoteCheckpointOptimizeSegments(CubeInstance cube, Map<Long, Long> recommendCuboids,
            CubeSegment... optimizedSegments) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            segAssist.promoteCheckpointOptimizeSegments(cube, recommendCuboids, optimizedSegments);
        }
    }

    public List<CubeSegment> calculateHoles(String cubeName) {
        return segAssist.calculateHoles(cubeName);
    }

    private class SegmentAssist {

        CubeSegment appendSegment(CubeInstance cube, TSRange tsRange, SegmentRange segRange,
                Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd)
                throws IOException {
            CubeInstance cubeCopy = cube.latestCopyForWrite(); // get a latest copy

            checkInputRanges(tsRange, segRange);

            // fix start/end a bit
            if (cubeCopy.getModel().getPartitionDesc().isPartitioned()) {
                // if missing start, set it to where last time ends
                if (tsRange != null && tsRange.start.v == 0) {
                    CubeDesc cubeDesc = cubeCopy.getDescriptor();
                    CubeSegment last = cubeCopy.getLastSegment();
                    if (last == null)
                        tsRange = new TSRange(cubeDesc.getPartitionDateStart(), tsRange.end.v);
                    else if (!last.isOffsetCube())
                        tsRange = new TSRange(last.getTSRange().end.v, tsRange.end.v);
                }
            } else {
                // full build
                tsRange = null;
                segRange = null;
            }

            CubeSegment newSegment = newSegment(cubeCopy, tsRange, segRange);
            newSegment.setSourcePartitionOffsetStart(sourcePartitionOffsetStart);
            newSegment.setSourcePartitionOffsetEnd(sourcePartitionOffsetEnd);
            validateNewSegments(cubeCopy, newSegment);

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToAddSegs(newSegment);
            updateCube(update);
            return newSegment;
        }

        public CubeSegment refreshSegment(CubeInstance cube, TSRange tsRange, SegmentRange segRange)
                throws IOException {
            CubeInstance cubeCopy = cube.latestCopyForWrite(); // get a latest copy

            checkInputRanges(tsRange, segRange);

            if (cubeCopy.getModel().getPartitionDesc().isPartitioned() == false) {
                // full build
                tsRange = null;
                segRange = null;
            }

            CubeSegment newSegment = newSegment(cubeCopy, tsRange, segRange);

            Pair<Boolean, Boolean> pair = cubeCopy.getSegments().fitInSegments(newSegment);
            if (pair.getFirst() == false || pair.getSecond() == false)
                throw new IllegalArgumentException("The new refreshing segment " + newSegment
                        + " does not match any existing segment in cube " + cubeCopy);

            if (segRange != null) {
                CubeSegment toRefreshSeg = null;
                for (CubeSegment cubeSegment : cubeCopy.getSegments()) {
                    if (cubeSegment.getSegRange().equals(segRange)) {
                        toRefreshSeg = cubeSegment;
                        break;
                    }
                }

                if (toRefreshSeg == null) {
                    throw new IllegalArgumentException(
                            "For streaming cube, only one segment can be refreshed at one time");
                }

                newSegment.setSourcePartitionOffsetStart(toRefreshSeg.getSourcePartitionOffsetStart());
                newSegment.setSourcePartitionOffsetEnd(toRefreshSeg.getSourcePartitionOffsetEnd());
            }

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToAddSegs(newSegment);
            updateCube(update);

            return newSegment;
        }

        public CubeSegment[] optimizeSegments(CubeInstance cube, Set<Long> cuboidsRecommend) throws IOException {
            CubeInstance cubeCopy = cube.latestCopyForWrite(); // get a latest copy

            List<CubeSegment> readySegments = cubeCopy.getSegments(SegmentStatusEnum.READY);
            CubeSegment[] optimizeSegments = new CubeSegment[readySegments.size()];
            int i = 0;
            for (CubeSegment segment : readySegments) {
                CubeSegment newSegment = newSegment(cubeCopy, segment.getTSRange(), null);
                validateNewSegments(cubeCopy, newSegment);

                optimizeSegments[i++] = newSegment;
            }

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setCuboidsRecommend(cuboidsRecommend);
            update.setToAddSegs(optimizeSegments);
            updateCube(update);

            return optimizeSegments;
        }

        public CubeSegment mergeSegments(CubeInstance cube, TSRange tsRange, SegmentRange segRange, boolean force)
                throws IOException {
            CubeInstance cubeCopy = cube.latestCopyForWrite(); // get a latest copy

            if (cubeCopy.getSegments().isEmpty())
                throw new IllegalArgumentException("Cube " + cubeCopy + " has no segments");

            checkInputRanges(tsRange, segRange);
            checkCubeIsPartitioned(cubeCopy);

            if (cubeCopy.getSegments().getFirstSegment().isOffsetCube()) {
                // offset cube, merge by date range?
                segRange = getOffsetCubeSegRange(cubeCopy, tsRange, segRange);
                tsRange = null;
                Preconditions.checkArgument(segRange != null);
            } else {
                /**In case of non-streaming segment,
                 * tsRange is the same as segRange,
                 * either could fulfill the merge job,
                 * so it needs to convert segRange to tsRange if tsRange is null.
                 **/
                if (tsRange == null) {
                    tsRange = new TSRange((Long) segRange.start.v, (Long) segRange.end.v);
                }
                segRange = null;
            }

            CubeSegment newSegment = newSegment(cubeCopy, tsRange, segRange);

            Segments<CubeSegment> mergingSegments = cubeCopy.getMergingSegments(newSegment);
            if (mergingSegments.size() <= 1)
                throw new IllegalArgumentException("Range " + newSegment.getSegRange()
                        + " must contain at least 2 segments, but there is " + mergingSegments.size());

            CubeSegment first = mergingSegments.get(0);
            CubeSegment last = mergingSegments.get(mergingSegments.size() - 1);
            if (force == false) {
                checkReadyForMerge(mergingSegments);
            }

            if (first.isOffsetCube()) {
                newSegment.setSegRange(new SegmentRange(first.getSegRange().start, last.getSegRange().end));
                newSegment.setSourcePartitionOffsetStart(first.getSourcePartitionOffsetStart());
                newSegment.setSourcePartitionOffsetEnd(last.getSourcePartitionOffsetEnd());
                newSegment.setTSRange(null);
            } else {
                newSegment.setTSRange(new TSRange(mergingSegments.getTSStart(), mergingSegments.getTSEnd()));
                newSegment.setSegRange(null);
            }

            validateNewSegments(cubeCopy, newSegment);

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToAddSegs(newSegment);
            updateCube(update);

            return newSegment;
        }

        private void checkReadyForMerge(Segments<CubeSegment> mergingSegments) {

            // check if the segments to be merged are continuous
            for (int i = 0; i < mergingSegments.size() - 1; i++) {
                if (!mergingSegments.get(i).getSegRange().connects(mergingSegments.get(i + 1).getSegRange()))
                    throw new IllegalStateException("Merging segments must not have gaps between "
                            + mergingSegments.get(i) + " and " + mergingSegments.get(i + 1));
            }

            // check if the segments to be merged are not empty
            List<String> emptySegment = Lists.newArrayList();
            for (CubeSegment seg : mergingSegments) {
                if (seg.getSizeKB() == 0 && seg.getInputRecords() == 0) {
                    emptySegment.add(seg.getName());
                }
            }

            if (emptySegment.size() > 0) {
                throw new IllegalArgumentException(
                        "Empty cube segment found, couldn't merge unless 'forceMergeEmptySegment' set to true: "
                                + emptySegment);
            }
        }

        private SegmentRange getOffsetCubeSegRange(CubeInstance cubeCopy, TSRange tsRange, SegmentRange segRange) {
            if (segRange == null && tsRange != null) {
                Pair<CubeSegment, CubeSegment> pair = cubeCopy.getSegments(SegmentStatusEnum.READY)
                        .findMergeOffsetsByDateRange(tsRange, Long.MAX_VALUE);
                if (pair == null)
                    throw new IllegalArgumentException(
                            "Find no segments to merge by " + tsRange + " for cube " + cubeCopy);
                segRange = new SegmentRange(pair.getFirst().getSegRange().start,
                        pair.getSecond().getSegRange().end);
            }
            return segRange;
        }

        private void checkInputRanges(TSRange tsRange, SegmentRange segRange) {
            if (tsRange != null && segRange != null) {
                throw new IllegalArgumentException(
                        "Build or refresh cube segment either by TSRange or by SegmentRange, not both.");
            }
        }

        private void checkCubeIsPartitioned(CubeInstance cube) {
            if (cube.getDescriptor().getModel().getPartitionDesc().isPartitioned() == false) {
                throw new IllegalStateException(
                        "there is no partition date column specified, only full build is supported");
            }
        }

        private CubeSegment newSegment(CubeInstance cube, TSRange tsRange, SegmentRange segRange) {
            DataModelDesc modelDesc = cube.getModel();

            CubeSegment segment = new CubeSegment();
            segment.setUuid(RandomUtil.randomUUID().toString());
            segment.setName(CubeSegment.makeSegmentName(tsRange, segRange, modelDesc));
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

        public void promoteNewlyBuiltSegments(CubeInstance cube, CubeSegment newSegCopy) throws IOException {
            // double check the updating objects are not on cache
            if (newSegCopy.getCubeInstance().isCachedAndShared())
                throw new IllegalStateException();

            CubeInstance cubeCopy = getCube(cube.getName()).latestCopyForWrite();

            if (StringUtils.isBlank(newSegCopy.getStorageLocationIdentifier()))
                throw new IllegalStateException(
                        String.format(Locale.ROOT, "For cube %s, segment %s missing StorageLocationIdentifier",
                                cubeCopy.toString(), newSegCopy.toString()));

            if (StringUtils.isBlank(newSegCopy.getLastBuildJobID()))
                throw new IllegalStateException(
                        String.format(Locale.ROOT, "For cube %s, segment %s missing LastBuildJobID",
                                cubeCopy.toString(), newSegCopy.toString()));

            if (isReady(newSegCopy) == true) {
                logger.warn("For cube {}, segment {} state should be NEW but is READY", cubeCopy, newSegCopy);
            }

            List<CubeSegment> tobe = cubeCopy.calculateToBeSegments(newSegCopy);

            if (tobe.contains(newSegCopy) == false)
                throw new IllegalStateException(
                        String.format(Locale.ROOT, "For cube %s, segment %s is expected but not in the tobe %s",
                                cubeCopy.toString(), newSegCopy.toString(), tobe.toString()));

            newSegCopy.setStatus(SegmentStatusEnum.READY);

            List<CubeSegment> toRemoveSegs = Lists.newArrayList();
            for (CubeSegment segment : cubeCopy.getSegments()) {
                if (!tobe.contains(segment))
                    toRemoveSegs.add(segment);
            }

            logger.info("Promoting cube {}, new segment {}, to remove segments {}", cubeCopy, newSegCopy, toRemoveSegs);

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToRemoveSegs(toRemoveSegs.toArray(new CubeSegment[toRemoveSegs.size()]))
                    .setToUpdateSegs(newSegCopy);
            if (cube.getConfig().isJobAutoReadyCubeEnabled()) {
                update.setStatus(RealizationStatusEnum.READY);
            }
            updateCube(update);
        }

        public void promoteNewlyOptimizeSegments(CubeInstance cube, CubeSegment... optimizedSegments)
                throws IOException {
            CubeInstance cubeCopy = cube.latestCopyForWrite();
            CubeSegment[] segCopy = cube.regetSegments(optimizedSegments);

            for (CubeSegment seg : segCopy) {
                seg.setStatus(SegmentStatusEnum.READY_PENDING);
            }

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToUpdateSegs(segCopy);
            updateCube(update);
        }

        public void promoteCheckpointOptimizeSegments(CubeInstance cube, Map<Long, Long> recommendCuboids,
                CubeSegment... optimizedSegments) throws IOException {
            CubeInstance cubeCopy = cube.latestCopyForWrite();
            CubeSegment[] optSegCopy = cubeCopy.regetSegments(optimizedSegments);

            if (cubeCopy.getSegments().size() != optSegCopy.length * 2) {
                throw new IllegalStateException(
                        String.format(Locale.ROOT, "For cube %s, every READY segment should be optimized and all segments should be READY before optimizing",
                                cubeCopy.toString()));
            }

            CubeSegment[] originalSegments = new CubeSegment[optSegCopy.length];
            int i = 0;
            for (CubeSegment seg : optSegCopy) {
                originalSegments[i++] = cubeCopy.getOriginalSegmentToOptimize(seg);

                if (StringUtils.isBlank(seg.getStorageLocationIdentifier()))
                    throw new IllegalStateException(
                            String.format(Locale.ROOT, "For cube %s, segment %s missing StorageLocationIdentifier",
                                    cubeCopy.toString(), seg.toString()));

                if (StringUtils.isBlank(seg.getLastBuildJobID()))
                    throw new IllegalStateException(
                            String.format(Locale.ROOT, "For cube %s, segment %s missing LastBuildJobID",
                                    cubeCopy.toString(), seg.toString()));

                seg.setStatus(SegmentStatusEnum.READY);
            }

            logger.info("Promoting cube {}, new segments {}, to remove segments {}",
                         cubeCopy, Arrays.toString(optSegCopy), originalSegments);

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToRemoveSegs(originalSegments) //
                    .setToUpdateSegs(optSegCopy) //
                    .setCuboids(recommendCuboids) //
                    .setCuboidsRecommend(Sets.<Long> newHashSet());
            if (cube.getConfig().isJobAutoReadyCubeEnabled()) {
                update.setStatus(RealizationStatusEnum.READY);
            }
            updateCube(update);
        }

        private void validateNewSegments(CubeInstance cube, CubeSegment newSegments) {
            List<CubeSegment> tobe = cube.calculateToBeSegments(newSegments);
            List<CubeSegment> newList = Arrays.asList(newSegments);
            if (tobe.containsAll(newList) == false) {
                throw new IllegalStateException(
                        String.format(Locale.ROOT, "For cube %s, the new segments %s do not fit in its current %s; the resulted tobe is %s",
                                cube.toString(), newList.toString(), cube.getSegments().toString(), tobe.toString()));
            }
        }

        /**
         * Calculate the holes (gaps) in segments.
         * @param cubeName
         * @return
         */
        public List<CubeSegment> calculateHoles(String cubeName) {
            List<CubeSegment> holes = Lists.newArrayList();
            final CubeInstance cube = getCube(cubeName);
            DataModelDesc modelDesc = cube.getModel();
            Preconditions.checkNotNull(cube);
            final List<CubeSegment> segments = cube.getSegments();
            logger.info("totally {} cubeSegments", segments.size());
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
                        hole.setName(CubeSegment.makeSegmentName(null, hole.getSegRange(), modelDesc));
                    } else {
                        hole.setTSRange(new TSRange(first.getTSRange().end.v, second.getTSRange().start.v));
                        hole.setName(CubeSegment.makeSegmentName(hole.getTSRange(), null, modelDesc));
                    }
                    holes.add(hole);
                }
            }
            return holes;
        }

    }

    // ============================================================================
    // Dictionary/Snapshot related methods
    // ============================================================================

    public DictionaryInfo buildDictionary(CubeSegment cubeSeg, TblColRef col, IReadableTable inpTable)
            throws IOException {
        return dictAssist.buildDictionary(cubeSeg, col, inpTable);
    }

    public DictionaryInfo saveDictionary(CubeSegment cubeSeg, TblColRef col, IReadableTable inpTable,
            Dictionary<String> dict) throws IOException {
        return dictAssist.saveDictionary(cubeSeg, col, inpTable, dict);
    }

    /**
     * return null if no dictionary for given column
     */
    public Dictionary<String> getDictionary(CubeSegment cubeSeg, TblColRef col) {
        return dictAssist.getDictionary(cubeSeg, col);
    }

    public SnapshotTable buildSnapshotTable(CubeSegment cubeSeg, String lookupTable, String uuid) throws IOException {
        return dictAssist.buildSnapshotTable(cubeSeg, lookupTable, uuid);
    }

    private TableMetadataManager getMetadataManager() {
        return TableMetadataManager.getInstance(config);
    }

    private class DictionaryAssist {
        public DictionaryInfo buildDictionary(CubeSegment cubeSeg, TblColRef col, IReadableTable inpTable)
                throws IOException {
            CubeDesc cubeDesc = cubeSeg.getCubeDesc();
            if (!cubeDesc.getAllColumnsNeedDictionaryBuilt().contains(col))
                return null;

            String builderClass = cubeDesc.getDictionaryBuilderClass(col);
            DictionaryInfo dictInfo = getDictionaryManager().buildDictionary(col, inpTable, builderClass);

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

        private void saveDictionaryInfo(CubeSegment cubeSeg, TblColRef col, DictionaryInfo dictInfo)
                throws IOException {
            if (dictInfo == null)
                return;

            // work on copy instead of cached objects
            CubeInstance cubeCopy = cubeSeg.getCubeInstance().latestCopyForWrite(); // get a latest copy
            CubeSegment segCopy = cubeCopy.getSegmentById(cubeSeg.getUuid());

            Dictionary<?> dict = dictInfo.getDictionaryObject();
            segCopy.putDictResPath(col, dictInfo.getResourcePath());
            segCopy.getRowkeyStats().add(new Object[] { col.getIdentity(), dict.getSize(), dict.getSizeOfId() });

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToUpdateSegs(segCopy);
            updateCube(update);
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
                throw new IllegalStateException("Failed to get dictionary for cube segment" + cubeSeg + ", col" + col,
                        e);
            }
            return (Dictionary<String>) info.getDictionaryObject();
        }

        public SnapshotTable buildSnapshotTable(CubeSegment cubeSeg, String lookupTable, String uuid) throws IOException {
            // work on copy instead of cached objects
            CubeInstance cubeCopy = cubeSeg.getCubeInstance().latestCopyForWrite(); // get a latest copy
            CubeSegment segCopy = cubeCopy.getSegmentById(cubeSeg.getUuid());

            TableMetadataManager metaMgr = getTableManager();
            SnapshotManager snapshotMgr = getSnapshotManager();

            TableDesc tableDesc = new TableDesc(metaMgr.getTableDesc(lookupTable, segCopy.getProject()));
            IReadableTable hiveTable = SourceManager.createReadableTable(tableDesc, uuid);
            SnapshotTable snapshot = snapshotMgr.buildSnapshot(hiveTable, tableDesc, cubeSeg.getConfig());

            CubeDesc cubeDesc = cubeSeg.getCubeDesc();
            if (!cubeDesc.isGlobalSnapshotTable(lookupTable)) {
                segCopy.putSnapshotResPath(lookupTable, snapshot.getResourcePath());
                CubeUpdate update = new CubeUpdate(cubeCopy);
                update.setToUpdateSegs(segCopy);
                updateCube(update);

                // Update the input cubeSeg after the resource store updated
                cubeSeg.putSnapshotResPath(lookupTable, segCopy.getSnapshotResPath(lookupTable));
            } else {
                CubeUpdate cubeUpdate = new CubeUpdate(cubeCopy);
                Map<String, String> map = Maps.newHashMap();
                map.put(lookupTable, snapshot.getResourcePath());
                cubeUpdate.setUpdateTableSnapshotPath(map);
                updateCube(cubeUpdate);

                cubeSeg.getCubeInstance().putSnapshotResPath(lookupTable, snapshot.getResourcePath());
            }
            return snapshot;
        }
    }

}
