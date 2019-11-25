/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.kylin.common.util.ClassUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SpanningTreeFactory {
    //TODO: KapConfig
    public static SpanningTree fromLayouts(Collection<LayoutEntity> layoutEntities, String cacheKey) {
        Map<IndexEntity, Collection<LayoutEntity>> descLayouts = getIndexEntity2Layouts(layoutEntities);
        return fromIndexes(descLayouts, cacheKey);
    }

    private static SpanningTree fromIndexes(Map<IndexEntity, Collection<LayoutEntity>> cuboids, String cacheKey) {
        return newInstance(KapConfig.getInstanceFromEnv(), cuboids, cacheKey);
    }

    private static SpanningTree newInstance(KapConfig kapConfig, Map<IndexEntity, Collection<LayoutEntity>> cuboids,
            String cacheKey) {
        try {
            String clzName = kapConfig.getCuboidSpanningTree();
            Class<? extends SpanningTree> clz = ClassUtil.forName(clzName, SpanningTree.class);
            return clz.getConstructor(Map.class, String.class).newInstance(cuboids, cacheKey);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<IndexEntity, Collection<LayoutEntity>> getIndexEntity2Layouts(
            Collection<LayoutEntity> layoutEntities) {
        Map<IndexEntity, Collection<LayoutEntity>> descLayouts = Maps.newHashMap();
        for (LayoutEntity layout : layoutEntities) {
            IndexEntity cuboidDesc = layout.getIndexEntity();
            if (descLayouts.get(cuboidDesc) == null) {
                Set<LayoutEntity> layouts = Sets.newHashSet();
                layouts.add(layout);
                descLayouts.put(cuboidDesc, layouts);
            } else {
                descLayouts.get(cuboidDesc).add(layout);
            }
        }
        return descLayouts;
    }
}
