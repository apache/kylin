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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.base.Function;
import org.apache.kylin.guava30.shaded.common.collect.Collections2;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import lombok.val;

@Deprecated
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NForestSpanningTreeV2 extends NForestSpanningTree {

    private static final Logger logger = LoggerFactory.getLogger(NForestSpanningTreeV2.class);

    public NForestSpanningTreeV2(Map<IndexEntity, Collection<LayoutEntity>> cuboids, String cacheKey) {
        super(cuboids, cacheKey);
    }

    private List<TreeNode> getAllLeafNodes() {
        val roots = getRoots();
        List<TreeNode> leafNodes = Lists.newArrayList();
        roots.forEach(root -> leafNodes.addAll(getTreeLeafNodes(root)));
        return leafNodes;
    }

    private List<TreeNode> getTreeLeafNodes(TreeNode node) {
        List<TreeNode> nodes = Lists.newArrayList();
        if (node.children.isEmpty()) {
            nodes.add(node);
        } else {
            node.children.forEach(child -> nodes.addAll(getTreeLeafNodes(child)));
        }
        return nodes;
    }

    @Override
    public Collection<IndexEntity> decideTheNextBatch(NDataSegment segment) {
        List<TreeNode> nextBatchIndex = Lists.newArrayList();
        // Smaller cuboid has smaller cost, and has higher priority when finding children.
        Comparator<IndexEntity> c1 = Comparator.comparingLong(o -> getRows(o, segment));

        // for deterministic
        Comparator<IndexEntity> c2 = Comparator.comparingLong(IndexEntity::getId);

        List<TreeNode> leafNodes = getAllLeafNodes();
        val orderedIndexes = leafNodes.stream().map(leaf -> leaf.indexEntity) //
                .filter(index -> isBuilt(index, segment)) //
                .sorted(c1.thenComparing(c2)) //
                .collect(Collectors.toList());

        orderedIndexes.forEach(index -> {
            nextBatchIndex.addAll(adjustTree(index, segment, false).children);

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

        return Collections2.transform(nextBatchIndex, new Function<TreeNode, IndexEntity>() {
            @Override
            public IndexEntity apply(NSpanningTree.TreeNode node) {
                return node.indexEntity;
            }
        });
    }
}
