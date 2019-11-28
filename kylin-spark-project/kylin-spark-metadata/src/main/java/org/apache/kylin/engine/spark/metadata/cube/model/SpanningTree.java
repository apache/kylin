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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class SpanningTree implements Serializable {
    final protected Map<IndexEntity, Collection<LayoutEntity>> cuboids;

    public SpanningTree(Map<IndexEntity, Collection<LayoutEntity>> cuboids) {
        long totalSize = 0L;
        for (Collection<LayoutEntity> entities : cuboids.values()) {
            totalSize += entities.size();
        }
        long maxCombination = KylinConfig.getInstanceFromEnv().getCubeAggrGroupMaxCombination() * 10;
        Preconditions.checkState(totalSize <= maxCombination,
                "Too many cuboids for the cube. Cuboid combination reached " + totalSize + " and limit is "
                        + maxCombination + ". Abort calculation.");
        this.cuboids = cuboids;
    }

    abstract public boolean isValid(long cuboidId);

    abstract public int getCuboidCount();

    abstract public Collection<IndexEntity> getRootIndexEntities();

    abstract public Collection<LayoutEntity> getLayouts(IndexEntity cuboidDesc);

    abstract public IndexEntity getIndexEntity(long cuboidId);

    abstract public LayoutEntity getCuboidLayout(long cuboidLayoutId);

    abstract public Collection<IndexEntity> getChildrenByIndexPlan(IndexEntity parent);

    abstract public Collection<IndexEntity> getAllIndexEntities();

    public Map<IndexEntity, Collection<LayoutEntity>> getCuboids() {
        return cuboids;
    }

    public static class TreeNode implements Serializable {
        @JsonProperty("cuboid")
        protected final IndexEntity indexEntity;

        @JsonProperty("children")
        protected final ArrayList<TreeNode> children = Lists.newArrayList();

        @JsonProperty("level")
        protected int level;

        protected transient TreeNode parent;
        protected transient List<IndexEntity> parentCandidates;
        protected transient boolean hasBeenDecided = false;

        public TreeNode(IndexEntity indexEntity) {
            this.indexEntity = indexEntity;
        }

        @Override
        public String toString() {
            return "level:" + level + ", node:" + indexEntity.getId() + //
                    ", dim:" + indexEntity.getDimensionBitset().toString() + //
                    ", measure:" + indexEntity.getMeasureBitset().toString() + //
                    ", children:{" + children.toString() + "}";//
        }
    }

    abstract public LayoutEntity getCuboidLayout(long cuboidLayoutId);

    abstract public Collection<IndexEntity> getRootIndexEntities();
}
