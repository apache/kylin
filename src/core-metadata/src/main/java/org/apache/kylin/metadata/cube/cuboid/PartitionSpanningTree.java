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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.LayoutPartition;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.job.JobBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class PartitionSpanningTree extends AdaptiveSpanningTree {

    private static final long serialVersionUID = 6066477814307614441L;

    private static final Logger logger = LoggerFactory.getLogger(PartitionSpanningTree.class);

    // Non-ancestor node mappings, improve search efficiency, read only.
    private transient Map<Long, Map<Long, PartitionTreeNode>> cachedNodeMap;

    public PartitionSpanningTree(KylinConfig config, PartitionTreeBuilder builder) {
        super(config, builder);
    }

    @Override
    public List<IndexEntity> getLevel0thIndices() {
        return level0thNodes.stream().map(TreeNode::getIndex).distinct().collect(Collectors.toList());
    }

    @Override
    public List<IndexEntity> getIndices() {
        return treeNodes.stream().map(TreeNode::getIndex).distinct().collect(Collectors.toList());
    }

    public List<Long> getFlatTablePartitions() {
        return level0thNodes.stream().filter(TreeNode::parentIsNull).map(PartitionTreeNode.class::cast) //
                .map(PartitionTreeNode::getPartition).distinct().sorted().collect(Collectors.toList());
    }

    @Override
    protected void buildMappings(List<TreeNode> nodes) {
        final Map<Long, Map<Long, PartitionTreeNode>> mappings = Maps.newHashMap();
        nodes.stream().map(PartitionTreeNode.class::cast).forEach(node -> {
            Map<Long, PartitionTreeNode> partitionMap = mappings.get(node.getIndex().getId());
            if (Objects.isNull(partitionMap)) {
                partitionMap = Maps.newHashMap();
                mappings.put(node.getIndex().getId(), partitionMap);
            }
            partitionMap.put(node.getPartition(), node);
        });
        this.cachedNodeMap = Collections.unmodifiableMap(mappings);
    }

    @Override
    protected List<TreeNode> adaptiveSpan(final NDataSegment dataSegment) {

        final Comparator<PartitionCandidate> comparator = Comparator.comparingInt(PartitionCandidate::getParentLevel) //
                .thenComparingDouble(PartitionCandidate::getParentUnfinishedFraction) //
                .thenComparingLong(PartitionCandidate::getParentRows) //
                .thenComparingLong(PartitionCandidate::getIndexId) //
                .thenComparingLong(PartitionCandidate::getPartition);

        return treeNodes.stream().filter(TreeNode::nonSpanned).map(PartitionTreeNode.class::cast).map(node -> {
            if (node.getDirectParents().isEmpty()) {
                // Level 0th node's parent has been initialized at TreeBuilder#build.
                PartitionCandidate candidate = new PartitionCandidate(node, null, null, null);
                candidate.setFraction(1.0d);
                return candidate;
            }
            return getOptimalCandidate(getParentCandidates(node, dataSegment));
        }).filter(Objects::nonNull).sorted(comparator).limit(adaptiveBatchSize).map(this::markSpanned) //
                .collect(Collectors.toList());
    }

    @Override
    protected List<TreeNode> layeredSpan(final NDataSegment dataSegment) {
        final Comparator<PartitionCandidate> comparator = //
                Comparator.comparingLong(PartitionCandidate::getIndexId)
                        .thenComparingLong(PartitionCandidate::getPartition);
        return treeNodes.stream().filter(TreeNode::nonSpanned).map(PartitionTreeNode.class::cast).map(node -> {
            if (node.getDirectParents().isEmpty()) {
                // Level 0th node's parent has been initialized at TreeBuilder#build.
                PartitionCandidate candidate = new PartitionCandidate(node, null, null, null);
                candidate.setFraction(1.0d);
                return candidate;
            }

            List<PartitionCandidate> parents = getParentCandidates(node, dataSegment);
            if (parents.size() < node.getDirectParents().size()) {
                // If not all direct parents were completed, the node wouldn't be spanned.
                return null;
            }
            return parents.stream().min(Comparator.comparingLong(PartitionCandidate::getParentRows)).orElse(null);
        }).filter(Objects::nonNull).sorted(comparator).map(this::markSpanned).collect(Collectors.toList());
    }

    private PartitionTreeNode getPartitionNode(IndexEntity index, Long partition) {
        Preconditions.checkNotNull(index, "Index shouldn't be null.");
        Preconditions.checkNotNull(partition, "Partition shouldn't be null.");
        Preconditions.checkNotNull(cachedNodeMap, "Node mappings' cache shouldn't be null.");
        Map<Long, PartitionTreeNode> partitionMap = cachedNodeMap.get(index.getId());
        if (Objects.isNull(partitionMap)) {
            return null;
        }
        return partitionMap.get(partition);
    }

    private List<PartitionCandidate> getParentCandidates(PartitionTreeNode node, //
            final NDataSegment dataSegment) {
        final Long partition = node.partition;
        return node.getDirectParents().stream() //
                .map(index -> getPartitionNode(index, partition)).filter(Objects::nonNull) //
                .map(parent -> parent.getLayouts().stream() //
                        .map(layout -> getLayoutPartition(layout, partition, dataSegment)).filter(Objects::nonNull) //
                        .filter(pair -> checkLayoutPartitionNewBucketCompleted(parent, pair))
                        .findAny().map(pair -> new PartitionCandidate(node, parent, pair.getFirst(), pair.getSecond())) //
                        .orElse(null)) //
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private boolean checkLayoutPartitionNewBucketCompleted(PartitionTreeNode node, Pair<NDataLayout, LayoutPartition> pair) {
        LayoutPartition lp = pair.getSecond();
        return node.matchNewBucket(lp.getBucketId());
    }

    private static Pair<NDataLayout, LayoutPartition> getLayoutPartition(LayoutEntity layout, Long partition, //
            final NDataSegment dataSegment) {
        NDataLayout dataLayout = dataSegment.getLayout(layout.getId());
        if (Objects.isNull(dataLayout)) {
            return null;
        }
        LayoutPartition dataPartition = dataLayout.getDataPartition(partition);
        if (Objects.isNull(dataPartition)) {
            return null;
        }
        return new Pair<>(dataLayout, dataPartition);
    }

    public static class PartitionTreeBuilder extends AdaptiveTreeBuilder {

        private final String jobId;

        private final List<Long> partitions;

        private final Set<JobBucket> newBuckets;

        public PartitionTreeBuilder(NDataSegment dataSegment, Collection<LayoutEntity> layouts, //
                String jobId, List<Long> partitions, Set<JobBucket> newBuckets) {
            super(dataSegment, layouts);
            Preconditions.checkNotNull(jobId, "Job id shouldn't be null.");
            Preconditions.checkNotNull(partitions, "Partitions shouldn't be null.");
            this.jobId = jobId;
            this.partitions = Collections.unmodifiableList(partitions);
            this.newBuckets = newBuckets;
        }

        @Override
        protected List<TreeNode> buildTreeNodes() {
            // <index, <partition, node>>
            final Map<Long, Map<Long, PartitionTreeNode>> ancestorNodeMap = Maps.newHashMap();
            return Collections.unmodifiableList(indexLayoutsMap.entrySet().stream().flatMap(indexLayouts -> { //
                IndexEntity index = indexLayouts.getKey();
                List<LayoutEntity> layouts = indexLayouts.getValue();
                List<IndexEntity> directParents = getDirectParents(index);

                List<PartitionTreeNode> nodes = partitions.stream().map(partition -> {
                    Set<Long> layoutIds = layouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
                    Set<Long> newBucketIds = newBuckets.stream().filter(nb -> layoutIds.contains(nb.getLayoutId()))
                            .map(JobBucket::getBucketId)
                            .collect(Collectors.toSet());

                    return new PartitionTreeNode(index, layouts, partition, newBucketIds);
                }).collect(Collectors.toList());
                if (directParents.isEmpty()) {
                    final List<IndexEntity> candidates = indexPlanIndices.stream() //
                            .filter(parent -> parent.fullyDerive(index)) //
                            .filter(parent -> parent.getLayouts().stream().anyMatch(layout -> //
                    Objects.nonNull(dataSegment.getLayout(layout.getId())))).collect(Collectors.toList());
                    nodes.forEach(node -> {
                        node.level = 0;
                        Pair<NDataLayout, LayoutPartition> layoutPartition = candidates.stream() //
                                .map(parent -> parent.getLayouts().stream() //
                                        .map(layout -> getLayoutPartition(layout, node.partition, dataSegment)) //
                                        .filter(Objects::nonNull).findAny().orElse(null))
                                .filter(Objects::nonNull) //
                                .min(Comparator.comparingLong(pair -> pair.getSecond().getRows())).orElse(null);

                        if (Objects.nonNull(layoutPartition)) {
                            PartitionTreeNode ancestor = //
                                    getAncestorNode(node, layoutPartition.getFirst().getLayout(), ancestorNodeMap);
                            ancestor.subtrees.add(node);
                            node.parent = ancestor;
                            node.rootNode = ancestor;
                        }
                    });
                } else {
                    nodes.forEach(node -> node.directParents = directParents);
                }

                // In case of node resumed from checkpoint
                nodes.forEach(this::markSpannedIfResumed);
                return nodes.stream();
            }).collect(Collectors.toList()));
        }

        private PartitionTreeNode getAncestorNode(PartitionTreeNode node, LayoutEntity layout, //
                Map<Long, Map<Long, PartitionTreeNode>> ancestorNodeMap) {
            Map<Long, PartitionTreeNode> partitionNodeMap = ancestorNodeMap.get(layout.getIndex().getId());
            if (Objects.isNull(partitionNodeMap)) {
                partitionNodeMap = Maps.newHashMap();
                ancestorNodeMap.put(layout.getIndex().getId(), partitionNodeMap);
            }
            PartitionTreeNode ancestor = partitionNodeMap.get(node.partition);
            if (Objects.isNull(ancestor)) {
                ancestor = new PartitionTreeNode(layout.getIndex(), Lists.newArrayList(layout), node.partition);
                ancestor.layout = layout;
                ancestor.layoutNodes.forEach(LayoutNode::setSpanned);
                partitionNodeMap.put(node.partition, ancestor);
            }
            return ancestor;
        }

        private void markSpannedIfResumed(PartitionTreeNode node) {
            node.layoutNodes.forEach(lnode -> {
                // Data layout.
                NDataLayout dataLayout = dataSegment.getLayout(lnode.getLayout().getId());
                if (Objects.isNull(dataLayout)) {
                    return;
                }

                // Data partition.
                LayoutPartition dataPartition = dataLayout.getDataPartition(node.getPartition());
                if (Objects.isNull(dataPartition)) {
                    return;
                }

                // Job id.
                if (jobId.equals(dataPartition.getBuildJobId())) {
                    // Mark spanned.
                    lnode.setSpanned();
                    logger.info("Segment {} skip build layout partition {} {}", dataSegment.getId(), //
                            lnode.getLayout().getId(), node.getPartition());
                }
            });
        }
    }

    public static class PartitionTreeNode extends TreeNode {

        // Every partition may have its particular parent.
        private final Long partition;
        private final Set<Long> newBucketIds;

        public PartitionTreeNode(IndexEntity index, List<LayoutEntity> layouts, Long partition) {
            this(index, layouts, partition, null);
        }

        public PartitionTreeNode(IndexEntity index, List<LayoutEntity> layouts, Long partition, Set<Long> newBucketIds) {
            super(index, layouts);
            Preconditions.checkNotNull(partition, "Partition shouldn't be null.");
            this.partition = partition;
            this.newBucketIds = newBucketIds;
        }

        public Long getPartition() {
            return partition;
        }

        public boolean matchNewBucket(Long bucketId) {
            return newBucketIds != null && newBucketIds.contains(bucketId);
        }
    }

    private static class PartitionCandidate extends Candidate {

        private final LayoutPartition dataPartition;

        public PartitionCandidate(PartitionTreeNode node, PartitionTreeNode parent, NDataLayout dataLayout, //
                LayoutPartition dataPartition) {
            super(node, parent, dataLayout);
            Preconditions.checkState((dataLayout == null) == (dataPartition == null), //
                    "Both dataLayout and dataPartition must be defined, or neither.");
            this.dataPartition = dataPartition;
        }

        private Long getPartition() {
            return ((PartitionTreeNode) getNode()).getPartition();
        }

        @Override
        protected Long getParentRows() {
            if (Objects.isNull(dataPartition)) {
                return -1L;
            }
            return dataPartition.getRows();
        }

        @Override
        protected String getReadableDesc() {
            return "partition " + getPartition() + ", " + super.getReadableDesc();
        }
    }
}
