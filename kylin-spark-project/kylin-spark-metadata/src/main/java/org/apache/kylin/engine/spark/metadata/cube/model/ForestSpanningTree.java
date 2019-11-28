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
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Function;

public class ForestSpanningTree extends SpanningTree {
    // IndexEntity <> TreeNode
    @JsonProperty("nodes")
    private Map<Long, TreeNode> nodesMap = Maps.newTreeMap();

    private final Map<Long, LayoutEntity> layoutMap = Maps.newHashMap();

    /* If base cuboid exists, forest will become tree. */
    @JsonProperty("roots")
    private final List<TreeNode> roots = Lists.newArrayList();

    private static final Logger logger = LoggerFactory.getLogger(ForestSpanningTree.class);

    private static final Function<TreeNode, IndexEntity> TRANSFORM_FUNC = new Function<TreeNode, IndexEntity>() {
        @Nullable
        @Override
        public IndexEntity apply(@Nullable TreeNode input) {
            return input == null ? null : input.indexEntity;
        }
    };

    public ForestSpanningTree(Map<IndexEntity, Collection<LayoutEntity>> cuboids) {
        super(cuboids);
        init();
    }

    @Override
    public boolean isValid(long requestCuboid) {
        return nodesMap.containsKey(requestCuboid);
    }

    @Override
    public int getCuboidCount() {
        return nodesMap.size();
    }

    @Override
    public Collection<IndexEntity> getRootIndexEntities() {
        return Collections2.transform(roots, TRANSFORM_FUNC::apply);
    }

    @Override
    public Collection<LayoutEntity> getLayouts(IndexEntity indexEntity) {
        return cuboids.get(indexEntity);
    }

    @Override
    public IndexEntity getIndexEntity(long cuboidId) {
        if (nodesMap.get(cuboidId) == null) {
            throw new IllegalStateException("Cuboid（ID:" + cuboidId + ") does not exist!");
        }
        return nodesMap.get(cuboidId).indexEntity;
    }

    @Override
    public LayoutEntity getCuboidLayout(long cuboidLayoutId) {
        return layoutMap.get(cuboidLayoutId);
    }

    @Override
    public Collection<IndexEntity> getChildrenByIndexPlan(IndexEntity parent) {
        // only meaningful when parent has been called in decideTheNextLayer
        TreeNode parentNode = nodesMap.get(parent.getId());
        Preconditions.checkState(parentNode.hasBeenDecided, "Node must have been decided before get its children.");
        return Collections2.transform(parentNode.children, TRANSFORM_FUNC::apply);
    }

    @Override
    public Collection<IndexEntity> getAllIndexEntities() {
        return Collections2.transform(nodesMap.values(), TRANSFORM_FUNC::apply);
    }

    private void init() {
        new TreeBuilder(cuboids.keySet()).build();
    }


    private class TreeBuilder {
        // Sort in descending order of dimension and measure number to make sure children is in front
        // of parent.
        private SortedSet<IndexEntity> sortedCuboids = Sets.newTreeSet((o1, o2) -> {
            int c = Integer.compare(o1.getDimensions().size(), o2.getDimensions().size());
            if (c != 0)
                return c;
            else
                return Long.compare(o1.getId(), o2.getId());
        });

        private TreeBuilder(Collection<IndexEntity> cuboids) {
            if (cuboids != null)
                this.sortedCuboids.addAll(cuboids);
        }

        private void build() {
            for (IndexEntity cuboid : sortedCuboids) {
                addCuboid(cuboid);
            }
        }

        private void addCuboid(IndexEntity cuboid) {
            TreeNode node = new TreeNode(cuboid);
            List<IndexEntity> candidates = findDirectParentCandidates(cuboid);
            if (!candidates.isEmpty()) {
                node.parentCandidates = candidates;
            } else {
                node.level = 0;
                roots.add(node);
            }

            nodesMap.put(cuboid.getId(), node);
            for (LayoutEntity layout : cuboid.getLayouts()) {
                layoutMap.put(layout.getId(), layout);
            }
        }

        // decide every cuboid's direct parent candidates(eg ABCD->ABC->AB, ABCD is ABC's direct parent, but not AB's).
        // but will not decide the cuboid tree.
        // when in building, will find the best one as the cuboid's parent.
        private List<IndexEntity> findDirectParentCandidates(IndexEntity entity) {
            List<IndexEntity> candidates = new ArrayList<>();
            for (IndexEntity cuboid : sortedCuboids) {

                if (!cuboid.fullyDerive(entity)) {
                    continue;
                }

                // only add direct parent
                // [XY's Note] ABC, ABD are both parent candidates for AB, while ABCD will not be considered if ABC exists
                if (candidates.stream().noneMatch(candidate -> cuboid.fullyDerive(candidate))) {
                    candidates.add(cuboid);
                }
            }

            return candidates;
        }
    }
}
