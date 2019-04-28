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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.model.ISegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class CubeL2Cache {

    private static final Logger LOG = LoggerFactory.getLogger(CubeL2Cache.class);

    private CubeManager cubeMgr;
    private Map<String, CubeCache> cubeCaches = new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);


    public CubeL2Cache(CubeManager cubeMgr) {
        this.cubeMgr = cubeMgr;
    }

    // cat
    public List<MeasureInstance> getMeasuresOnCubeAllowMiss(String cubeName) {
        CubeCache cache = cubeCaches.get(cubeName);
        cache = cache == null ? reloadCacheAllowMiss(cubeName) : cache;
        if (cache == null) {
            return Collections.EMPTY_LIST;
        }
        return ImmutableList.copyOf(cache.measures);
    }

    public List<MeasureInstance> getMeasuresOnCube(String cubeName) {
        CubeCache cache = getCubeCache(cubeName);
        if (cache == null) {
            return Collections.EMPTY_LIST;
        }
        return ImmutableList.copyOf(cache.measures);
    }
    public List<MeasureInstance> getMeasuresOf(String cubeName, String segmentName) {
        CubeCache cache = getCubeCache(cubeName);
        if (cache == null) {
            return Collections.EMPTY_LIST;
        }
        List<MeasureInstance> ret = cache.segmentMeasureMap.get(segmentName);
        if (null == ret) {
            return Collections.EMPTY_LIST;
        }
        return ImmutableList.copyOf(ret);
    }

    private CubeCache getCubeCache(String cubeName) {
        CubeCache cache = cubeCaches.get(cubeName);
        cache = cache == null ? reloadCache(cubeName) : cache;
        return cache;
    }

    // delete
    public CubeCache remove(String cubeName) {
        return cubeCaches.remove(cubeName);
    }

    // add
    public CubeCache reloadCache(String cube) {
        LOG.debug("Reloading L2 cube cache for " + cube);
        CubeCache cubeCache = new CubeCache(cube);

        CubeInstance cubeInstance = cubeMgr.getCube(cube);
        if (cubeInstance == null) {
            return null;
        }
        loadMeasureOnCube(cubeCache, cubeInstance);
        cubeCaches.put(cube, cubeCache);
        return cubeCache;
    }

    public CubeCache reloadCacheAllowMiss(String cube) {
        LOG.debug("Reloading L2 cube cache(allow miss) for " + cube);
        CubeCache cubeCache = new CubeCache(cube);

        CubeInstance cubeInstance = cubeMgr.getCube(cube);
        loadMeasureOnCube(cubeCache, cubeInstance, true);
        cubeCaches.put(cube, cubeCache);
        return cubeCache;
    }

    private void loadMeasureOnCube(CubeCache cubeCache, CubeInstance cubeInstance) {
        loadMeasureOnCube(cubeCache, cubeInstance, false);
    }

    private void loadMeasureOnCube(CubeCache cubeCache, CubeInstance cubeInstance, boolean allowMiss) {

        List<String> measureNameFromCube = cubeInstance.getMeasures().stream().map(m -> m.getName()).collect(Collectors.toList());
        List<String> measureKeyFromL1Cache = getMeasureKeyFromCache(cubeInstance.getName());
        for (String mk : measureKeyFromL1Cache) {
            MeasureInstance measure = cubeMgr.getMeasureManager().getMeasureByKey(mk);
            if (null != measure) {
                cubeCache.measures.add(measure);
                // update segment map
                for (ISegment seg : measure.getSegments()) {
                    if (cubeCache.segmentMeasureMap.get(seg.getName()) == null) {
                        List<MeasureInstance> measuresOverSeg = Lists.newArrayList();
                        measuresOverSeg.add(measure);
                        cubeCache.segmentMeasureMap.put(seg.getName(), measuresOverSeg);
                    } else {
                        cubeCache.segmentMeasureMap.get(seg.getName()).add(measure);
                    }
                }
            } else {
                String msg = String.format(Locale.ROOT, "Measure %s is not found in L1 cache now.", mk);
                LOG.warn(msg);
            }
        }
        if (cubeCache.measures.size() != measureNameFromCube.size()) {
            List<String> measureNameFromL2Cache = cubeCache.measures.stream().map(m -> m.getName()).collect(Collectors.toList());
            String msg = String.format(Locale.ROOT, "measures in cube %s are different with measures in L2 cahe %s", measureNameFromCube, measureNameFromL2Cache);
            if (allowMiss) {
                LOG.warn(msg);
            } else {
                throw new MissMeasureCacheException(msg);
            }
        }
    }

    private List<String> getMeasureKeyFromCache(String name) {
        Set<String> cacheKey = cubeMgr.getMeasureManager().getCache().keySet();
        String pre = name + "/";
        return cacheKey.stream().filter(s -> s.startsWith(pre)).collect(Collectors.toList());
    }

    private List<String> getMeasureKeyFromCubeCache(CubeInstance cubeInstance) {
        return cubeInstance.getMeasures().stream()
                .map(m -> MeasureInstance.getResourceName(cubeInstance.getName(), m.getName()))
                .collect(Collectors.toList());
    }


    public static class MissMeasureCacheException extends RuntimeException {
        public MissMeasureCacheException(Exception e) {
            super(e);
        }

        public MissMeasureCacheException(String msg) {
            super(msg);
        }
    }

    private static class CubeCache {
        private String cubeName;
        private List<MeasureInstance> measures;
        // segment name => MeasureInstance s
        private Map<String, List<MeasureInstance>> segmentMeasureMap;

        CubeCache(String cubeName) {
            this.cubeName = cubeName;
            this.measures = Lists.newArrayList();
            this.segmentMeasureMap = Maps.newHashMap();
        }
    }

}
