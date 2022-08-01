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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * i. Layer controlling;
 * ii. Effective resource utilization.
 *
 *          [A,B,C,D]
 *  - - - - - - - - - - - - - -
 *       /      |     \
 *  [A,B,C]  [A,B,D]  [A,C,D]
 *     /       |         \
 *  [A,B]    [B,D]      [C,D]
 *    |      /   \        |
 *   [A]   [B]   [D]     [C]
 *
 * 1. Build [A,B] without waiting for [A,C,D] to complete;
 * 2. When [A,B,C] has completed, [A,B,D] is still under building, and the application has available resources at this time, it's time to initiate the building job of [A,B];
 * 3. When [A,B,C], [A,B,D] have completed, if rows([A,B,C]) < rows([A,B,D]), then [A,B] is based on [A,B,C] to complete.
 *
 */
public class AdaptiveSpanningTree implements Serializable {

    private static final long serialVersionUID = 5981664173055110627L;

    private static final Logger logger = LoggerFactory.getLogger(AdaptiveSpanningTree.class);

    // All non-ancestor spanning-tree nodes, read only.
    protected final transient List<TreeNode> treeNodes;

    // All level 0th nodes, read only.
    // Non-ancestor nodes.
    protected final transient List<TreeNode> level0thNodes;

    // Worked when 'adaptive' enabled.
    protected final double adaptiveThreshold;
    // Worked when 'adaptive' enabled.
    protected final int adaptiveBatchSize;

    protected final String segmentId;

    // Adaptive spanning-tree enabled.
    private final boolean adaptive;

    // Non-ancestor node mappings, improve search efficiency, read only.
    private transient Map<Long, TreeNode> cachedNodeMap;

    // Every segment has its spanning tree.
    public AdaptiveSpanningTree(KylinConfig config, AdaptiveTreeBuilder builder) {

        // Default: disabled
        this.adaptive = config.isAdaptiveSpanningTreeEnabled();
        this.adaptiveThreshold = config.getAdaptiveSpanningTreeThreshold();
        this.adaptiveBatchSize = config.getAdaptiveSpanningTreeBatchSize();

        // Log segment id.
        this.segmentId = builder.getSegmentId();

        // Divide the layer from bottom-up.
        this.treeNodes = builder.buildTreeNodes();

        // From flat table? from data layout?
        this.level0thNodes = getLevel0thNodes(treeNodes);

        // Build node mappings.
        buildMappings(treeNodes);
    }

    public boolean fromFlatTable() {
        return level0thNodes.stream().anyMatch(TreeNode::parentIsNull);
    }

    public boolean isSpanned() {
        return treeNodes.stream().allMatch(TreeNode::isSpanned);
    }

    public boolean nonSpanned() {
        return treeNodes.stream().anyMatch(TreeNode::nonSpanned);
    }

    public List<TreeNode> span(NDataSegment dataSegment) {
        if (adaptive) {
            return adaptiveSpan(dataSegment);
        }
        return layeredSpan(dataSegment);
    }

    public List<IndexEntity> getLevel0thIndices() {
        return level0thNodes.stream().map(TreeNode::getIndex).collect(Collectors.toList());
    }

    public List<IndexEntity> getIndices() {
        return treeNodes.stream().map(TreeNode::getIndex).collect(Collectors.toList());
    }

    public List<TreeNode> getFromFlatTableNodes() {
        return level0thNodes.stream().filter(TreeNode::parentIsNull).collect(Collectors.toList());
    }

    public List<TreeNode> getRootNodes() {
        return level0thNodes.stream().filter(TreeNode::parentNonNull) //
                .map(TreeNode::getParent).distinct().collect(Collectors.toList());
    }

    protected void buildMappings(List<TreeNode> nodes) {
        final Map<Long, TreeNode> mappings = Maps.newHashMap();
        nodes.forEach(node -> mappings.put(node.getIndex().getId(), node));
        this.cachedNodeMap = Collections.unmodifiableMap(mappings);
    }

    protected final <T extends Candidate> T getOptimalCandidate(List<T> parents) {
        if (parents.isEmpty()) {
            return null;
        }
        // Parents' spanning node are the same.
        int directParentSize = parents.get(0).getNode().getDirectParents().size();
        Preconditions.checkState(directParentSize > 0, "Direct parent size should be positive.");
        final double fraction = ((double) parents.size()) / directParentSize;
        if (fraction < adaptiveThreshold) {
            return null;
        }
        T candidate = parents.stream().min(Comparator.comparingLong(Candidate::getParentRows)).orElse(null);
        if (Objects.nonNull(candidate)) {
            candidate.setFraction(fraction);
        }
        return candidate;
    }

    protected List<TreeNode> adaptiveSpan(final NDataSegment dataSegment) {

        final Comparator<Candidate> comparator = Comparator.comparingInt(Candidate::getParentLevel) //
                .thenComparingDouble(Candidate::getParentUnfinishedFraction) //
                .thenComparingLong(Candidate::getParentRows) //
                .thenComparingInt(Candidate::getLocalPriority) //
                .thenComparingLong(Candidate::getIndexId);

        return treeNodes.stream().filter(TreeNode::nonSpanned) //
                .map(node -> {
                    if (node.getDirectParents().isEmpty()) {
                        // Level 0th node's parent has been initialized at TreeBuilder#build.
                        Candidate candidate = new Candidate(node, null, null);
                        candidate.setFraction(1.0d);
                        return candidate;
                    }
                    return getOptimalCandidate(getParentCandidates(node, dataSegment));
                }).filter(Objects::nonNull).sorted(comparator).limit(adaptiveBatchSize).map(this::markSpanned) //
                .collect(Collectors.toList());
    }

    protected List<TreeNode> layeredSpan(final NDataSegment dataSegment) {
        final Comparator<Candidate> comparator = Comparator.comparingLong(Candidate::getIndexId);
        return treeNodes.stream().filter(TreeNode::nonSpanned).map(node -> { //
            if (node.getDirectParents().isEmpty()) {
                // Level 0th node's parent has been initialized at TreeBuilder#build.
                Candidate candidate = new Candidate(node, null, null);
                candidate.setFraction(1.0d);
                return candidate;
            }
            List<Candidate> parents = getParentCandidates(node, dataSegment);
            if (parents.size() < node.getDirectParents().size()) {
                // If not all direct parents were completed, the node wouldn't be spanned.
                return null;
            }
            return parents.stream().min(Comparator.comparingLong(Candidate::getParentRows)).orElse(null);
        }).filter(Objects::nonNull).sorted(comparator).map(this::markSpanned).collect(Collectors.toList());
    }

    protected TreeNode markSpanned(Candidate candidate) {
        logger.info("Segment {} spanned node: {}", segmentId, candidate.getReadableDesc());
        TreeNode node = candidate.getNode();
        TreeNode parent = candidate.getParent();
        node.setSpanned();
        if (Objects.isNull(parent)) {
            return node;
        }
        parent.layout = candidate.getParentLayout();
        node.level = parent.level + 1;
        node.parent = parent;
        node.rootNode = parent.rootNode;
        parent.subtrees.add(node);
        return node;
    }

    private TreeNode getNode(IndexEntity index) {
        Preconditions.checkNotNull(index, "Index shouldn't be null.");
        Preconditions.checkNotNull(cachedNodeMap, "Node mappings' cache shouldn't be null.");
        return cachedNodeMap.get(index.getId());
    }

    private List<Candidate> getParentCandidates(TreeNode node, final NDataSegment dataSegment) {

        return node.getDirectParents().stream() //
                .map(this::getNode).filter(Objects::nonNull) //
                .map(parent -> parent.getLayouts().stream().map(layout -> dataSegment.getLayout(layout.getId())) //
                        .filter(Objects::nonNull).findAny().map(layout -> new Candidate(node, parent, layout)) //
                        .orElse(null)) //
                .filter(Objects::nonNull).collect(Collectors.toList());
    }

    private List<TreeNode> getLevel0thNodes(List<TreeNode> nodes) {
        final List<TreeNode> targets = nodes.stream().filter(node -> node.level == 0).collect(Collectors.toList());
        Map<Boolean, String> partitioned = Maps.transformValues(//
                targets.stream().collect(Collectors.partitioningBy(TreeNode::parentIsNull)), //
                partitionedNodes -> {
                    if (Objects.isNull(partitionedNodes)) {
                        // Empty nodes desc.
                        return "[]";
                    }
                    return partitionedNodes.stream().map(TreeNode::getIndex) //
                            .map(IndexEntity::getId) //
                            .distinct() // In case of partition nodes.
                            .sorted() //
                            .map(String::valueOf) //
                            .collect(Collectors.joining(",", "[", "]"));
                });
        // Essential log.
        logger.info("Segment {} nodes build from flat table {}, nodes build from data layout {}.", segmentId, //
                partitioned.get(true), partitioned.get(false));
        return Collections.unmodifiableList(targets);
    }

    public static class AdaptiveTreeBuilder {

        protected final NDataSegment dataSegment;
        protected final List<IndexEntity> indexPlanIndices;
        protected final Map<IndexEntity, List<LayoutEntity>> indexLayoutsMap;

        // Sequence like: [A], [B], [A,B], [A,C], [B,C], [A,B,C]
        protected final SortedSet<IndexEntity> sorted;

        // Used for initialization only, don't declare a global segment variable, segment would fulfill data layouts during tree-spanning.
        public AdaptiveTreeBuilder(NDataSegment dataSegment, Collection<LayoutEntity> layouts) {

            Preconditions.checkNotNull(dataSegment, "Data segment shouldn't be null.");
            Preconditions.checkNotNull(layouts, "Layouts shouldn't be null.");

            this.dataSegment = dataSegment;

            this.indexLayoutsMap = getIndexLayoutsMap(layouts);

            // heavy invocation
            this.indexPlanIndices = dataSegment.getIndexPlan().getAllIndexes();

            SortedSet<IndexEntity> sortedSet0 = Sets.newTreeSet((i1, i2) -> {
                int c = Integer.compare(i1.getDimensions().size(), i2.getDimensions().size());
                if (c == 0) {
                    return Long.compare(i1.getId(), i2.getId());
                }
                return c;
            });
            sortedSet0.addAll(indexLayoutsMap.keySet());
            sorted = Collections.unmodifiableSortedSet(sortedSet0);
        }

        protected List<TreeNode> buildTreeNodes() {
            final Map<Long, TreeNode> ancestorNodeMap = Maps.newHashMap();
            return Collections.unmodifiableList(indexLayoutsMap.entrySet().stream().map(indexLayouts -> {
                IndexEntity index = indexLayouts.getKey();
                List<LayoutEntity> layouts = indexLayouts.getValue();
                TreeNode node = new TreeNode(index, layouts);
                List<IndexEntity> directParents = getDirectParents(index);
                if (directParents.isEmpty()) {
                    node.level = 0;
                    NDataLayout dataLayout = indexPlanIndices.stream().filter(parent -> parent.fullyDerive(index)) //
                            .map(parent -> parent.getLayouts().stream() //
                                    .map(layout -> dataSegment.getLayout(layout.getId())).filter(Objects::nonNull) //
                                    .findAny().orElse(null)) //
                            .filter(Objects::nonNull) //
                            .min(Comparator.comparingLong(NDataLayout::getRows)).orElse(null);
                    if (Objects.nonNull(dataLayout)) {
                        // Ancestor node not included in spanning tree.
                        TreeNode ancestor = getAncestorNode(dataLayout.getLayout(), ancestorNodeMap);
                        ancestor.subtrees.add(node);
                        // Ancestor could be level 0th node's parent.
                        node.parent = ancestor;
                        // Only ancestor could be a root node.
                        node.rootNode = ancestor;
                    }
                } else {
                    node.directParents = directParents;
                }

                // In case of node resumed from checkpoint
                node.layoutNodes.forEach(lnode -> {
                    if (Objects.nonNull(dataSegment.getLayout(lnode.layout.getId()))) {
                        lnode.setSpanned();
                        logger.info("Segment {} skip build layout {}", dataSegment.getId(), lnode.layout.getId());
                    }
                });

                return node;
            }).collect(Collectors.toList()));
        }

        private String getSegmentId() {
            return dataSegment.getId();
        }

        private Map<IndexEntity, List<LayoutEntity>> getIndexLayoutsMap(Collection<LayoutEntity> layouts) {
            final Map<IndexEntity, List<LayoutEntity>> mappings = Maps.newHashMap();
            layouts.forEach(layout -> {
                IndexEntity index = layout.getIndex();
                List<LayoutEntity> mappedLayouts = mappings.get(index);
                if (Objects.isNull(mappedLayouts)) {
                    mappedLayouts = Lists.newArrayList();
                    mappings.put(index, mappedLayouts);
                }
                mappedLayouts.add(layout);
            });
            return Collections.unmodifiableMap(mappings);
        }

        private TreeNode getAncestorNode(LayoutEntity layout, Map<Long, TreeNode> ancestorNodeMap) {
            // ancestor node not included in spanning tree.
            TreeNode ancestor = ancestorNodeMap.get(layout.getIndex().getId());
            if (Objects.isNull(ancestor)) {
                ancestor = new TreeNode(layout.getIndex(), Lists.newArrayList(layout));
                ancestor.layout = layout;
                ancestor.layoutNodes.forEach(LayoutNode::setSpanned);
                ancestorNodeMap.put(layout.getIndex().getId(), ancestor);
            }
            return ancestor;
        }

        protected final List<IndexEntity> getDirectParents(IndexEntity index) {
            final List<IndexEntity> candidates = Lists.newArrayList();
            sorted.stream().filter(e -> e.fullyDerive(index)).forEach(e -> {
                if (e.equals(index)) {
                    return;
                }
                if (candidates.stream().anyMatch(e::fullyDerive)) {
                    return;
                }
                candidates.add(e);
            });
            return Collections.unmodifiableList(candidates);
        }
    }

    public static class TreeNode {

        // Flat table: null
        protected TreeNode parent;

        // Flat table: null
        // Existed data layout: virtual tree node, not included in the spanning-tree.
        protected TreeNode rootNode;

        // Subtrees' parent data layout.
        // Only parent nodes should maintain this.
        // By default, get the first one.
        protected LayoutEntity layout;

        protected final IndexEntity index;
        protected final List<LayoutNode> layoutNodes;

        protected int level = -1;

        protected final List<TreeNode> subtrees = Lists.newArrayList();

        // Level 0th nodes have no direct parents, they may have parent node.
        protected List<IndexEntity> directParents = Collections.emptyList();

        // Maintained by inferior flat table if enabled.
        // Locality of reference principle.
        protected int localPriority = -1;

        public TreeNode(IndexEntity index, List<LayoutEntity> layouts) {

            Preconditions.checkNotNull(index);
            Preconditions.checkNotNull(layouts);
            Preconditions.checkArgument(!layouts.isEmpty(), //
                    "No spanning-tree layout in index " + index.getId());

            this.index = index;
            this.layoutNodes = layouts.stream().map(LayoutNode::new).collect(Collectors.toList());
        }

        public boolean parentIsNull() {
            return Objects.isNull(parent);
        }

        public boolean parentNonNull() {
            return !parentIsNull();
        }

        public boolean isSpanned() {
            return layoutNodes.stream().allMatch(LayoutNode::isSpanned);
        }

        public boolean nonSpanned() {
            return layoutNodes.stream().anyMatch(LayoutNode::nonSpanned);
        }

        public void setSpanned() {
            layoutNodes.forEach(LayoutNode::setSpanned);
        }

        public TreeNode getParent() {
            return parent;
        }

        public TreeNode getRootNode() {
            return rootNode;
        }

        public List<IndexEntity> getDirectParents() {
            return directParents;
        }

        public IndexEntity getIndex() {
            return index;
        }

        public LayoutEntity getLayout() {
            Preconditions.checkNotNull(layout, "Parent data layout shouldn't be null.");
            return layout;
        }

        public List<LayoutEntity> getLayouts() {
            return Collections.unmodifiableList(//
                    layoutNodes.stream().map(LayoutNode::getLayout).collect(Collectors.toList()));
        }

        public List<TreeNode> getSubtrees() {
            return subtrees;
        }

        public int getNonSpannedCount() {
            return layoutNodes.stream().filter(LayoutNode::nonSpanned).map(e -> 1).reduce(0, Integer::sum);
        }

        public int getDimensionSize() {
            return index.getEffectiveDimCols().size();
        }

        public void setLocalPriority(int localPriority) {
            this.localPriority = localPriority;
        }

        public int getLocalPriority() {
            return localPriority;
        }
    }

    protected static class LayoutNode {

        private final LayoutEntity layout;

        // 'Spanned' doesn't mean the node's data layout built.
        protected boolean spanned = false;

        public LayoutNode(LayoutEntity layout) {
            this.layout = layout;
        }

        protected boolean isSpanned() {
            return spanned;
        }

        protected boolean nonSpanned() {
            return !spanned;
        }

        protected void setSpanned() {
            spanned = true;
        }

        protected LayoutEntity getLayout() {
            return layout;
        }
    }

    protected static class Candidate {

        private final TreeNode node;

        // Level 0th node's parent should has been specified at TreeBuilder#build.
        // Null parent here doesn't make any sense.
        private final TreeNode parent;
        private final NDataLayout dataLayout;
        private double fraction;

        protected Candidate(TreeNode node, TreeNode parent, NDataLayout dataLayout) {
            Preconditions.checkNotNull(node);
            Preconditions.checkState((parent == null) == (dataLayout == null), //
                    "Both parent and dataLayout must be defined, or neither.");
            this.node = node;
            this.parent = parent;
            this.dataLayout = dataLayout;
        }

        protected TreeNode getNode() {
            return node;
        }

        protected TreeNode getParent() {
            return parent;
        }

        protected Long getIndexId() {
            return node.index.getId();
        }

        protected Integer getParentLevel() {
            if (Objects.isNull(parent)) {
                return -1;
            }
            return parent.level;
        }

        protected LayoutEntity getParentLayout() {
            Preconditions.checkNotNull(dataLayout, "Parent data layout shouldn't be null.");
            return dataLayout.getLayout();
        }

        protected Long getParentRows() {
            if (Objects.isNull(dataLayout)) {
                return -1L;
            }
            return dataLayout.getRows();
        }

        protected void setFraction(double fraction) {
            this.fraction = fraction;
        }

        protected Double getParentUnfinishedFraction() {
            return 1.0d - fraction;
        }

        protected Integer getLocalPriority() {
            return node.getLocalPriority();
        }

        protected String getReadableDesc() {
            return "index " + node.getIndex().getId() //
                    + ", optimal parent " + getParentDesc() //
                    + ", parent level " + getParentLevel() //
                    + ", completion rate " + String.format(Locale.ROOT, "%.3f", fraction) //
                    + ", direct parents " + getDirectParentsDesc();
        }

        private String getParentDesc() {
            // Non level 0th, from data layout.
            if (Objects.nonNull(parent)) {
                return String.valueOf(dataLayout.getLayout().getId());
            }

            // Level 0th.
            if (Objects.isNull(node.getParent())) {
                // From flat table.
                return "flat table";
            }
            // From data layout.
            return String.valueOf(node.getParent().getLayout().getId());
        }

        private String getDirectParentsDesc() {
            return node.getDirectParents().stream().map(IndexEntity::getId).map(String::valueOf) //
                    .collect(Collectors.joining(",", "[", "]"));
        }
    }
}
