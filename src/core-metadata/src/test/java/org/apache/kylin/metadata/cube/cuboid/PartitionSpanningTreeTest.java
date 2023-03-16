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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TestUtils;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(project = "multi_level_partition")
class PartitionSpanningTreeTest {

    private static final String PROJECT = "multi_level_partition";
    private static final String MODEL_ID = "53abfc64-e4f3-b284-8c65-aac59c9c54c5";

    private PartitionSpanningTree tree;
    private KylinConfig config;
    private NDataModelManager dataModelManager;
    private NIndexPlanManager indexPlanManager;
    private NDataflowManager dataflowManager;

    @BeforeEach
    public void beforeEach() {
        config = TestUtils.getTestConfig();
        dataModelManager = NDataModelManager.getInstance(config, PROJECT);
        indexPlanManager = NIndexPlanManager.getInstance(config, PROJECT);
        dataflowManager = NDataflowManager.getInstance(config, PROJECT);
    }

    @Test
    void testLayeredSpan() {
        NDataModel dataModel = dataModelManager.getDataModelDesc(MODEL_ID);
        List<Long> partitions = dataModel.getMultiPartitionDesc()
                .getPartitions().stream()
                .map(MultiPartitionDesc.PartitionInfo::getId)
                .collect(Collectors.toList());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(MODEL_ID);
        List<LayoutEntity> layouts = indexPlan.getAllLayouts();
        NDataflow dataflow = dataflowManager.getDataflow(MODEL_ID);
        NDataSegment dataSegment = dataflow.getFirstSegment();

        Set<JobBucket> newBuckets = new HashSet<>();
        long partitionId = partitions.get(0);
        long maxBucketId = dataSegment.getMaxBucketId();
        for (LayoutEntity layout : layouts) {
            newBuckets.add(new JobBucket(dataSegment.getId(), layout.getId(), maxBucketId + 1, partitionId));
            ++maxBucketId;
        }

        tree = new PartitionSpanningTree(config,
                new PartitionSpanningTree.PartitionTreeBuilder(dataSegment, layouts, "a_job_id", partitions, newBuckets));

        {
            List<AdaptiveSpanningTree.TreeNode> nodes = tree.layeredSpan(dataSegment);
            assertEquals(2, nodes.size());
        }
    }

    @Test
    void testPartitionTreeNode_matchNewBucket() {
        NDataModel dataModel = dataModelManager.getDataModelDesc(MODEL_ID);
        List<Long> partitions = dataModel.getMultiPartitionDesc()
                .getPartitions().stream()
                .map(MultiPartitionDesc.PartitionInfo::getId)
                .collect(Collectors.toList());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(MODEL_ID);

        List<LayoutEntity> layouts = indexPlan.getAllLayouts();
        IndexEntity indexEntity = layouts.get(0).getIndex();
        long partitionId = partitions.get(0);
        Set<Long> newBucketsIds = Sets.newHashSet(0L, 1L);

        {
            PartitionSpanningTree.PartitionTreeNode node = new PartitionSpanningTree.PartitionTreeNode(indexEntity, layouts, partitionId, null);
            assertFalse(node.matchNewBucket(0L));
        }

        {
            PartitionSpanningTree.PartitionTreeNode node = new PartitionSpanningTree.PartitionTreeNode(indexEntity, layouts, partitionId, newBucketsIds);
            assertTrue(node.matchNewBucket(0L));
            assertFalse(node.matchNewBucket(2L));
        }
    }
}
