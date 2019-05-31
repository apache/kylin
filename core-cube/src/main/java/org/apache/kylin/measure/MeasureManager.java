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

package org.apache.kylin.measure;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import static org.apache.kylin.metadata.cachesync.Broadcaster.Event.CREATE;
import static org.apache.kylin.metadata.cachesync.Broadcaster.Event.DROP;
import static org.apache.kylin.metadata.cachesync.Broadcaster.Event.UPDATE;

public class MeasureManager {

    public static final Serializer<MeasureInstance> MEASURE_SERIALIZER = new JsonSerializer<>(MeasureInstance.class);

    private static final Logger LOG = LoggerFactory.getLogger(MeasureManager.class);
    public static final String CUBE_MEASURE = "cube_measure";

    public static MeasureManager getInstance(KylinConfig config) {
        return config.getManager(MeasureManager.class);
    }

    // called by reflection
    static MeasureManager newInstance(KylinConfig config) throws IOException {
        return new MeasureManager(config);
    }

    // ===============================================

    private KylinConfig config;

    // measure instance store path ==> MeasureInstance
    private CaseInsensitiveStringCache<MeasureInstance> measureMap;
    private CubeL2Cache cubeL2Cache;

    private CachedCrudAssist<MeasureInstance> crud;

    private AutoReadWriteLock cubeMapLock;

    private MeasureManager(KylinConfig config) throws IOException {
        LOG.info("Initializing MeasureManager with config " + config);
        this.config = config;
        this.measureMap = new CaseInsensitiveStringCache<>(config, "measure");
        this.cubeL2Cache = new CubeL2Cache(getCubeManager());
        this.cubeMapLock = getCubeManager().getLock();
        this.crud = new CachedCrudAssist<MeasureInstance>(getStore(), ResourceStore.MEASURE_RESOURCE_ROOT, MeasureInstance.class, measureMap) {
            @Override
            protected MeasureInstance initEntityAfterReload(MeasureInstance entity, String resourceName) {
                CubeInstance cube = getCubeManager().getCube(entity.getCubeName());
                entity.init(cube);

                return entity;
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new MeasureSyncListener(), CUBE_MEASURE);
    }

    private class MeasureSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey) throws IOException {
            if (entity.equals(CUBE_MEASURE)) {
                triggerByCube(broadcaster, entity, event, cacheKey);
            } else {
                throw new IllegalArgumentException("Only listen on " + CUBE_MEASURE + ", can't process this entity: " + entity);
            }
        }

        private void triggerByCube(Broadcaster broadcaster, String cube, Broadcaster.Event event, String cubeName) throws IOException {
            if (event == DROP) {
                removeLocalByCube(cubeName);
            } else {
                reloadByCubeQuietly(cubeName);
            }
        }

        private void triggerByMeasure(Broadcaster broadcaster, String measure, Broadcaster.Event event, String measureKey) {
            if (event == DROP) {
                removeLocal(measureKey);
            } else {
                reloadQuietly(measureKey);
            }
        }

    }

    private MeasureInstance reloadQuietly(String cacheKey) {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            MeasureInstance ret = crud.reloadQuietly(cacheKey);
            cubeL2Cache.reloadCache(ret.getCubeName());
            return ret;
        }
    }

    private List<MeasureInstance> reloadByCubeQuietly(String cubeName) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            CubeInstance cube = getCubeManager().getCube(cubeName);
            if (cube == null) {
                LOG.warn("Can't find cube {} in cache, it's missing or unload yet, so kylin can't reload cache of measures on cube. you can trigger this reload process again by update this cube!");
                return Collections.EMPTY_LIST;
            }
            // remove L1 cache
            getMeasuresInCubeAllowMiss(cube.getProject(), cube.getName()).stream()
                    .forEach(m -> measureMap.removeLocal(m.getKey()));

            List<String> paths = getStore().collectResourceRecursively(ResourceStore.MEASURE_RESOURCE_ROOT + "/" + cube.getName(), MetadataConstants.FILE_SURFIX);

            List<MeasureInstance> ret = Lists.newArrayListWithCapacity(paths.size());
            paths.forEach(p -> ret.add(crud.reloadAt(p)));
            cubeL2Cache.reloadCache(cubeName);
            return ImmutableList.copyOf(ret);
        }
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public MeasureInstance getMeasure(String cubeName, String measureName) {
        try (AutoLock lock = cubeMapLock.lockForRead()) {
            return getMeasureByKey(MeasureInstance.getResourceName(cubeName, measureName));
        }
    }

    public MeasureInstance getMeasureByKey(String key) {
        try (AutoLock lock = cubeMapLock.lockForRead()) {
            return measureMap.get(key);
        }
    }

    public List<MeasureInstance> getMeasuresOnSegment(String cubeName, String segmentName) {
        try (AutoLock lock = cubeMapLock.lockForRead()) {
            return cubeL2Cache.getMeasuresOf(cubeName, segmentName);
        }
    }

    public List<MeasureInstance> getMeasuresInCube(String cubeName) {
        try (AutoLock lock = cubeMapLock.lockForRead()) {
            return cubeL2Cache.getMeasuresOnCube(cubeName);
        }
    }

    /**
     * Sometimes, CubeDesc in cache contain more measures than in Measures in cache, or vice versa.
     * this call return cache may miss those measures. Those measure will be restored after CubeDesc update
     * @param projectName
     * @param cubeName
     * @return
     */
    public List<MeasureInstance> getMeasuresInCubeAllowMiss(String projectName, String cubeName) {
        try (AutoLock lock = cubeMapLock.lockForRead()) {
            // try get don't allow miss
            return cubeL2Cache.getMeasuresOnCube(cubeName);
        } catch (CubeL2Cache.MissMeasureCacheException e) {
            LOG.info("There have some missing measures. cause by: " + e.getLocalizedMessage());
            return cubeL2Cache.getMeasuresOnCubeAllowMiss(cubeName);
        }
    }

    /**
     * create and announce
     * @param cube
     * @return
     * @throws IOException
     */
    public List<MeasureInstance> createMeasuresOnCube(CubeInstance cube) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            boolean existed = cubeL2Cache.getMeasuresOnCubeAllowMiss(cube.getName()).size() != 0;
            List<MeasureDesc> measures = cube.getMeasures();
            List<MeasureInstance> needSaveMeasures = measures.stream()
                    .map(m -> MeasureInstance.createMeasureInstance(m, cube))
                    .collect(Collectors.toList());

            return batchSaveCubeMeasure(needSaveMeasures, cube.getName(), existed);
        }
    }

    // don't announce
    private MeasureInstance createMeasureAlone(MeasureInstance measureInstance) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            createInStore(measureInstance);
            // save in cache
            return createCache(measureInstance);
        }
    }

    private void createInStore(MeasureInstance measureInstance) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            // save meta data
            getStore().checkAndPutResource(getResourcePath(measureInstance), measureInstance, MEASURE_SERIALIZER);
        }
    }

    private MeasureInstance createCache(MeasureInstance measureInstance) {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            // save in cache
            measureMap.putLocal(measureInstance.getKey(), measureInstance);
            cubeL2Cache.reloadCache(measureInstance.getCubeName());
            return measureInstance;
        }
    }

    public List<MeasureInstance> batchSaveCubeMeasure(List<MeasureInstance> measures, String cubeName, boolean existed) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            // save in store
            batchSaveCubeMeasureInStore(measures);

            // announce
            announceCubeMeasureEvent(existed ? UPDATE : CREATE, cubeName);

            // save in cache
            return batchSaveCubeMeasureInCache(measures, cubeName);
        }
    }

    private List<MeasureInstance> batchSaveCubeMeasureInStore(List<MeasureInstance> measures) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            // save in store
            for (MeasureInstance m : measures) {
                if (null == m.getUuid()) {
                    m.updateRandomUuid();
                }
                createInStore(m);
            }
            return measures;
        }
    }

    private List<MeasureInstance> batchSaveCubeMeasureInCache(List<MeasureInstance> measures, String cubeName) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            // save in cache
            for (MeasureInstance m : measures) {
                measureMap.putLocal(m.getKey(), m);
            }
            cubeL2Cache.reloadCache(cubeName);

            return measures;
        }
    }

    // ================delete====================

    private MeasureInstance deleteInStore(MeasureInstance m) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            if (m.getSegmentsName().size() > 0) {
                throw new IllegalStateException(String.format(Locale.ROOT, "Can't delete measure %s, please delete the contained segments first. %s", m.getName(), m.getSegmentsName()));
            }
            getStore().deleteResource(getResourcePath(m));
            return m;
        }
    }

    private MeasureInstance deleteCache(MeasureInstance m) {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            measureMap.remove(m.getKey());
            cubeL2Cache.reloadCache(m.getCubeName());
            return m;
        }
    }

    public List<MeasureInstance> deleteByCube(String cubeName) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            // delete meta data
            List<MeasureInstance> needDrop = deleteInStoreByCube(cubeName);
            if (null != needDrop && needDrop.size() > 0) {
                // announce delete cube measure
                announceCubeMeasureEvent(DROP, cubeName);
                // delete in cache
                removeLocalByCube(cubeName);
            }
            return needDrop;
        }
    }

    private List<MeasureInstance> removeLocalByCube(String cubeName) {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            List<MeasureInstance> needRemove = cubeL2Cache.getMeasuresOnCube(cubeName);
            if (needRemove.size() == 0) {
                // get from L1 cache
                needRemove = getMeasuresInCubeFromL1Cache(cubeName);
            }
            // remove CubeL2Cache
            cubeL2Cache.remove(cubeName);
            // remove measureMap
            needRemove.forEach(m -> removeL1Cache(m.getKey()));
            return needRemove;
        }
    }

    private List<MeasureInstance> getMeasuresInCubeFromL1Cache(String cubeName) {
        String prefix = cubeName + "/";
        return measureMap.keySet()
                .stream()
                .filter(k -> k.startsWith(prefix))
                .map(k -> measureMap.get(k))
                .collect(Collectors.toList());
    }

    private MeasureInstance removeL1Cache(String key) {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            MeasureInstance ret = measureMap.get(key);
            if (null != ret) {
                measureMap.removeLocal(key);
            }
            return ret;
        }
    }

    /**
     * remove L1 cache and reload L2 cache by measure key
     * @param key
     * @return
     */
    private MeasureInstance removeLocal(String key) {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            MeasureInstance ret = removeL1Cache(key);
            if (null != ret) {
                cubeL2Cache.reloadCache(ret.getCubeName());
//                cubeL2Cache.reloadCacheAllowMiss(ret.getCubeName());
            }
            return ret;
        }
    }

    /**
     * just delete meta data in store, don't broadcast
     * @param cubeName
     * @return
     */
    private List<MeasureInstance> deleteInStoreByCube(String cubeName) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            List<MeasureInstance> needDrop = getMeasuresInCube(cubeName);
            ResourceStore store = getStore();
            for (MeasureInstance m : needDrop) {
                store.deleteResource(getResourcePath(m));
            }
            return needDrop;
        }
    }

    public void deleteSegmentByName(CubeInstance cube, String segmentName) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            List<MeasureInstance> updated = deleteSegInMeasure(cube, segmentName);
            batchSaveCubeMeasure(updated, cube.getName(), true);
        }
    }

    private List<MeasureInstance> deleteSegInMeasure(CubeInstance cube, String segmentName) {
        List<MeasureInstance> measuresOnSegment = getMeasuresOnSegment(cube.getName(), segmentName);
        if (measuresOnSegment.size() <= 0) {
            return measuresOnSegment;
        }
        List<MeasureInstance> updated = measuresOnSegment.stream()
                .map(m -> MeasureInstance.copy(m))
                .collect(Collectors.toList());
        for (MeasureInstance m : updated) {
            if (!m.getSegmentsName().remove(segmentName)) {
                LOG.warn("Can't find segment by name {} in measure {}.", segmentName, m.getKey());
                continue;
            }
            m.refreshSegments(cube);
        }
        return updated;
    }

    private List<MeasureInstance> batchDeleteSegInMeasure(CubeInstance cube, List<String> segmentsName) {
        List<MeasureInstance> measuresOnCube = getMeasuresInCube(cube.getName());
        if (measuresOnCube.size() <= 0) {
            return measuresOnCube;
        }
        List<MeasureInstance> updated = measuresOnCube.stream()
                .map(m -> MeasureInstance.copy(m))
                .collect(Collectors.toList());
        for (MeasureInstance m : updated) {
            if (m.getSegmentsName().removeIf(s -> segmentsName.contains(s))) {
                m.refreshSegments(cube);
            }
        }
        return updated;
    }

    /**
     *
     * if segsToDrop is null it's mean delete all;
     */
    public void deleteSegments(CubeInstance cube, ISegment... segsToDrop) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            List<String> needDrop = Lists.newArrayList(segsToDrop).stream().map(ISegment::getName).collect(Collectors.toList());
            deleteSegmentsByName(cube, needDrop);
        }
    }

    private void deleteSegmentsByName(CubeInstance cube, List<String> needDrop) throws IOException {
        // save in store
        List<MeasureInstance> updated = batchDeleteSegInMeasure(cube, needDrop);
        List<MeasureInstance> storedMeasures = batchSaveCubeMeasureInStore(updated);
        // announce
        announceUpdateCubeMeasure(cube.getName());
        // save in cache
        batchSaveCubeMeasureInCache(storedMeasures, cube.getName());
    }

    public void deleteAllSegments(CubeInstance cube) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            List<String> needDrop;
            LOG.info("Delete all segments in cube.", cube.getName());
            needDrop = cube.getSegments().stream().map(ISegment::getName).collect(Collectors.toList());

            // save in store
            deleteSegmentsByName(cube, needDrop);
        }
    }

    // ================update=====================
    /**
     * if some measures on cube have been modified. add or delete measure
     * @param cubeDesc
     * @return updated measures in cube
     */
    public List<MeasureInstance> updateMeasuresOnCube(CubeDesc cubeDesc) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            List<CubeInstance> cubesByDesc = getCubeManager().getCubesByDesc(cubeDesc.getName());
            List<MeasureInstance> ret = Lists.newArrayList();
            for (CubeInstance cube : cubesByDesc) {
                ret.addAll(updateMeasuresOnCube(cube));
            }

            return ret;
        }
    }

    public List<MeasureInstance> updateSegmentsOnCube(CubeInstance updatedCube, CubeUpdate update) throws IOException {
        ISegment[] toAddSegs = getReadySegments(update.getToAddSegs());
        ISegment[] toUpdateSegs = getReadySegments(update.getToUpdateSegs());
        ISegment[] toRemoveSegs = getReadySegments(update.getToRemoveSegs());

        boolean needToUpdateSegs = toAddSegs.length > 0 || toUpdateSegs.length > 0 || toRemoveSegs.length > 0;

        if (!needToUpdateSegs) {
            return Collections.emptyList();
        }
        /* there may have overlap between toRemoveSegs and toUpdateSegs,
         * because they are collected by their uuid.
         * So we remove toRemoveSegs firstly, then add toUpdateSegs.
         */
        List<MeasureInstance> newMeasures = batchDeleteSegInMeasure(updatedCube,
                Arrays.stream(toRemoveSegs).map(m -> m.getName()).collect(Collectors.toList()));

        Set<String> toAddSegNames = Arrays.stream(toAddSegs).map(ISegment::getName).collect(Collectors.toSet());
        Set<String> toUpdateSegNames = Arrays.stream(toUpdateSegs).map(ISegment::getName).collect(Collectors.toSet());

        for (int i = 0;
             (toAddSegNames.size() > 0 || toUpdateSegNames.size() > 0)
                     && i < newMeasures.size(); i++) {
            MeasureInstance measure = newMeasures.get(i);
            Set<String> newSegsName = Sets.newHashSet(measure.getSegmentsName());
            newSegsName.addAll(toAddSegNames);
            newSegsName.addAll(toUpdateSegNames);
            List<String> newSegsNameList = Lists.newArrayList(newSegsName);
            Collections.sort(newSegsNameList);
            measure.setSegmentsName(newSegsNameList);
            measure.refreshSegments(updatedCube);
        }
        batchSaveCubeMeasure(newMeasures, updatedCube.getName(), true);
        return newMeasures;
    }

    private ISegment[] getReadySegments(ISegment[] segs) {
        if (null == segs) {
            return new ISegment[]{};
        }
        return Arrays.stream(segs)
                .filter(s -> s.getStatus().equals(SegmentStatusEnum.READY))
                .toArray(ISegment[]::new);
    }

    private List<MeasureInstance> updateMeasuresOnCube(CubeInstance cube) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            List<MeasureInstance> cachedMeasureOnCube = getMeasuresInCubeAllowMiss(cube.getProject(), cube.getName());
            List<MeasureInstance> needDrop = Lists.newArrayListWithCapacity(5);
            List<MeasureInstance> needAdd = Lists.newArrayListWithCapacity(6);

            // find need drop
            Set<String> latestMeasures = cube.getMeasures().stream().map(MeasureDesc::getName).collect(Collectors.toSet());
            cachedMeasureOnCube.stream()
                    .filter(cm -> !latestMeasures.contains(cm.getName()))
                    .forEach(cm -> needDrop.add(cm));
            // find need add
            Set<String> curMeasureNames = cachedMeasureOnCube.stream().map(MeasureInstance::getName).collect(Collectors.toSet());
            cube.getMeasures().stream()
                    .filter(m -> !curMeasureNames.contains(m.getName()))
                    .forEach(m -> needAdd.add(MeasureInstance.createMeasureInstance(m, cube)));

            for (MeasureInstance m : needAdd) {
                createInStore(m);
            }

            for (MeasureInstance m : needDrop) {
                deleteInStore(m);
                removeLocal(m.getKey());
            }

            announceUpdateCubeMeasure(cube.getName());

            reloadByCubeQuietly(cube.getName());

            return getMeasuresInCube(cube.getName());
        }
    }

    /**
     *
     * @param cubeName
     * @param updatedMeasures
     * @return
     * @throws IOException
     */
    public List<MeasureInstance> updateMeasures(String cubeName, List<MeasureInstance> updatedMeasures) throws IOException {
        try (AutoLock lock = cubeMapLock.lockForWrite()) {
            return batchSaveCubeMeasure(updatedMeasures, cubeName, true);
        }
    }

    private String getResourcePath(MeasureInstance m) {
        return ResourceStore.MEASURE_RESOURCE_ROOT + "/" + m.resourceName() + MetadataConstants.FILE_SURFIX;
    }

    private CubeManager getCubeManager(){
        return CubeManager.getInstance(config);
    }

    private void announceUpdateCubeMeasure(String cubeName) {
        announceCubeMeasureEvent(UPDATE, cubeName);
    }

    private void announceCubeMeasureEvent(Broadcaster.Event e, String cubeName) {
        measureMap.getBroadcaster().announce(CUBE_MEASURE, e.getType(), cubeName);
    }

    public CaseInsensitiveStringCache<MeasureInstance> getCache() {
        return measureMap;
    }

    public static void main(String[] args) throws IOException {
        String errInputMsg =
                "Error argument. Usage: init cube cubeName \n" +
                "                            project projectName \n" +
                "                            all";
        if (args.length < 1 || !args[0].equals("init")) {
            LOG.error(errInputMsg);
            return;
        }
        switch (args[1]) {
            case "all":
                // init all cube
                initAllCube();
                break;
            case "project":
                // init by project
                String projectName = args[2];
                initByProject(projectName);
                break;
            case "cube":
                // init by project
                String cubeName = args[2];
                initialCubeMeasure(cubeName);
                break;
            default:
                LOG.error(errInputMsg);
                break;
        }

        return;
    }

    private static void initAllCube() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        List<CubeInstance> allCubInstance = cubeMgr.listAllCubes();
        for (CubeInstance cube : allCubInstance) {
            initialCubeMeasure(cube.getName());
        }
    }

    private static void initByProject(String projectName) throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        LOG.info("Initialize all cube measure under project " + projectName);
        ProjectInstance project = ProjectManager.getInstance(config).getProject(projectName);
        Preconditions.checkNotNull(project, "Can't find project " + projectName);
        List<RealizationEntry> realizationEntries = project.getRealizationEntries(RealizationType.CUBE);
        for (RealizationEntry r : realizationEntries) {
            String cubeName = r.getRealization();
            initialCubeMeasure(cubeName);
        }
    }

    private static void initialCubeMeasure(String cubeName) throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        if (null == cube) {
            LOG.error(String.format(Locale.ROOT, "Can't find cube by project: %s, cube: %s.", cubeName));
            return;
        }

        List<MeasureInstance> measures = cube.getMeasures().stream()
                .map(m -> MeasureInstance.createMeasureInstance(m, cube))
                .collect(Collectors.toList());
        List<String> segsName = cube.getSegments(SegmentStatusEnum.READY).stream()
                .map(ISegment::getName)
                .collect(Collectors.toList());
        MeasureManager measureManager = MeasureManager.getInstance(config);
        Iterator<MeasureInstance> iter = measures.iterator();

        while (iter.hasNext()) {
            MeasureInstance m = iter.next();
            MeasureInstance measureInStore = measureManager.getMeasure(m.getCubeName(), m.getName());
            if (null != measureInStore) {
                LOG.info(m.getKey() + " already exist, won't initialize it. you can delete it first.");
                m.setUuid(measureInStore.getUuid());
                m.setLastModified(measureInStore.getLastModified());
                iter.remove();
            }
            m.setSegmentsName(segsName);
            try {
                m.refreshSegments(cube);
            } catch (IllegalStateException e){
                LOG.error("Error: ", e);
                continue;
            }
        }

        measureManager.batchSaveCubeMeasureInStore(measures);
    }
}
