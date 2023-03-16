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

package org.apache.kylin.metadata.cube.cuboid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Collections2;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;

@Deprecated
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NForestSpanningTree extends NSpanningTree {
    @JsonProperty("nodes")
    protected final Map<Long, TreeNode> nodesMap = Maps.newTreeMap();
    protected final Map<Long, LayoutEntity> layoutMap = Maps.newHashMap();

    /* If base cuboid exists, forest will become tree. */
    @JsonProperty("roots")
    private final List<TreeNode> roots = Lists.newArrayList();

    private static final Function<TreeNode, IndexEntity> TRANSFORM_FUNC = new Function<TreeNode, IndexEntity>() {
        @Nullable
        @Override
        public IndexEntity apply(@Nullable TreeNode input) {
            return input == null ? null : input.indexEntity;
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(NForestSpanningTree.class);

    public NForestSpanningTree(Map<IndexEntity, Collection<LayoutEntity>> cuboids, String cacheKey) {
        super(cuboids, cacheKey);
        init();
    }

    public Map<Long, TreeNode> getNodesMap() {
        return nodesMap;
    }

    public List<TreeNode> getRoots() {
        return roots;
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
    public IndexEntity getIndexEntity(long indexId) {
        if (nodesMap.get(indexId) == null) {
            throw new IllegalStateException("Cuboid（ID:" + indexId + ") does not exist!");
        }
        return nodesMap.get(indexId).indexEntity;
    }

    @Override
    public LayoutEntity getLayoutEntity(long layoutId) {
        return layoutMap.get(layoutId);
    }

    @Override
    public void decideTheNextLayer(Collection<IndexEntity> currentLayer, NDataSegment segment) {
        // After built, we know each cuboid's size.
        // Then we will find each cuboid's children.
        // Smaller cuboid has smaller cost, and has higher priority when finding children.
        Comparator<IndexEntity> c1 = Comparator.comparingLong(o -> getRows(o, segment));

        // for deterministic
        Comparator<IndexEntity> c2 = Comparator.comparingLong(IndexEntity::getId);

        val orderedIndexes = currentLayer.stream() //
                .sorted(c1.thenComparing(c2)) //
                .collect(Collectors.toList()); //

        orderedIndexes.forEach(index -> {
            adjustTree(index, segment, true);

            logger.info("Adjust spanning tree." + //
            " Current index plan: {}." + //
            " Current index entity: {}." + //
            " Its children: {}\n", //
                    index.getIndexPlan().getUuid(), //
                    index.getId(), //
                    Arrays.toString(getChildrenByIndexPlan(index).stream() //
                            .map(IndexEntity::getId).toArray())//
            );
        });

    }

    @Override
    public Collection<IndexEntity> getChildrenByIndexPlan(IndexEntity parent) {
        // only meaningful when parent has been called in decideTheNextLayer
        TreeNode parentNode = nodesMap.get(parent.getId());
        Preconditions.checkState(parentNode.hasBeenDecided, "Node must have been decided before get its children.");
        return Collections2.transform(parentNode.children, TRANSFORM_FUNC::apply);
    }

    @Override
    public IndexEntity getParentByIndexEntity(IndexEntity child) {
        TreeNode childNode = nodesMap.get(child.getId());
        if (childNode.parent == null) {
            return null;
        }
        return childNode.parent.indexEntity;
    }

    @Override
    public IndexEntity getRootByIndexEntity(IndexEntity child) {
        TreeNode childNode = nodesMap.get(child.getId());
        return childNode.rootNode.indexEntity;
    }

    @Override
    public Collection<IndexEntity> getAllIndexEntities() {
        return Collections2.transform(nodesMap.values(), TRANSFORM_FUNC::apply);
    }

    @Override
    public Collection<IndexEntity> decideTheNextBatch(NDataSegment segment) {
        return null;
    }

    @Override
    public void addParentChildRelation(IndexEntity parent, IndexEntity child) {

        TreeNode parentNode = nodesMap.get(parent.getId());
        TreeNode childNode = nodesMap.get(child.getId());

        Preconditions.checkNotNull(childNode, String.format(Locale.ROOT, "child index:%d don't exist", child.getId()));

        if (parentNode == null) {
            //build from layout
            childNode.parent = new TreeNode(parent, true);
            childNode.rootNode = childNode.parent;
            childNode.level = 0;
            return;
        }

        childNode.parent = parentNode;
        childNode.rootNode = parentNode.rootNode;
        childNode.level = parentNode.level + 1;
    }

    protected TreeNode adjustTree(IndexEntity parent, NDataSegment seg, Boolean needParentsBuild) {
        TreeNode parentNode = nodesMap.get(parent.getId());

        List<TreeNode> children = nodesMap.values().stream() //
                .filter(node -> shouldBeAdded(node, parent, seg, needParentsBuild)).collect(Collectors.toList());//

        // update child node's parent.
        children.forEach(node -> {
            node.level = parentNode.level + 1;
            node.parent = parentNode;
            node.rootNode = parentNode.rootNode;
        });

        // update parent node's children.
        parentNode.children.addAll(children);
        parentNode.hasBeenDecided = true;
        return parentNode;
    }

    protected boolean shouldBeAdded(TreeNode node, IndexEntity parent, NDataSegment seg, Boolean needParentsBuild) {
        boolean shouldBeAdd = node.parent == null // already has been decided
                && node.parentCandidates != null //it is root node
                && node.parentCandidates.contains(parent); //its parents candidates did not contains this IndexEntity.
        if (needParentsBuild) {
            shouldBeAdd = shouldBeAdd && node.parentCandidates.stream().allMatch(c -> isBuilt(c, seg));
        }

        return shouldBeAdd;
    }

    public boolean isBuilt(IndexEntity ie, NDataSegment seg) {
        return getLayoutFromSeg(ie, seg) != null;
    }

    protected long getRows(IndexEntity ie, NDataSegment seg) {
        return getLayoutFromSeg(ie, seg).getRows();
    }

    private NDataLayout getLayoutFromSeg(IndexEntity ie, NDataSegment seg) {
        return seg.getLayout(Lists.newArrayList(getLayouts(ie)).get(0).getId());
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
                node.rootNode = node;
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
                if (candidates.stream().noneMatch(candidate -> cuboid.fullyDerive(candidate))) {
                    candidates.add(cuboid);
                }
            }

            return candidates;
        }
    }
}
