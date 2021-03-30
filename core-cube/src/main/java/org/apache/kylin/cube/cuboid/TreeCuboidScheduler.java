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

package org.apache.kylin.cube.cuboid;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class TreeCuboidScheduler extends CuboidScheduler {

    final private CuboidTree cuboidTree;

    public TreeCuboidScheduler(CubeDesc cubeDesc, List<Long> allCuboidIds, Comparator<Long> cuboidComparator) {
        super(cubeDesc);
        cuboidTree = CuboidTree.createFromCuboids(allCuboidIds, cuboidComparator);
    }

    @Override
    public Set<Long> getAllCuboidIds() {
        return cuboidTree.getAllCuboidIds();
    }

    @Override
    public int getCuboidCount() {
        return cuboidTree.getCuboidCount(Cuboid.getBaseCuboidId(cubeDesc));
    }

    @Override
    public List<Long> getSpanningCuboid(long cuboidId) {
        return cuboidTree.getSpanningCuboid(cuboidId);
    }

    @Override
    public long findBestMatchCuboid(long cuboidId) {
        return cuboidTree.findBestMatchCuboid(cuboidId);
    }

    @Override
    public boolean isValid(long requestCuboid) {
        return cuboidTree.isValid(requestCuboid);
    }

    public static class CuboidTree {
        private int treeLevels;

        private TreeNode root;

        private Comparator<Long> cuboidComparator;

        private Map<Long, TreeNode> index = new HashMap<>();

        @VisibleForTesting
        static CuboidTree createFromCuboids(List<Long> allCuboidIds) {
            return createFromCuboids(allCuboidIds, Cuboid.cuboidSelectComparator);
        }

        public static CuboidTree createFromCuboids(List<Long> allCuboidIds, Comparator<Long> cuboidComparator) {
            // sort the cuboid ids in descending order, so that don't need to adjust
            // the cuboid tree when adding cuboid id to the tree.
            Collections.sort(allCuboidIds, new Comparator<Long>() {
                @Override
                public int compare(Long o1, Long o2) {
                    return Long.compare(o2, o1);
                }
            });
            long basicCuboidId = allCuboidIds.get(0);
            CuboidTree cuboidTree = new CuboidTree(cuboidComparator);
            cuboidTree.setRoot(basicCuboidId);

            for (long cuboidId : allCuboidIds) {
                cuboidTree.addCuboid(cuboidId);
            }
            cuboidTree.buildIndex();
            return cuboidTree;
        }

        private CuboidTree(Comparator<Long> cuboidComparator) {
            this.cuboidComparator = cuboidComparator;
        }

        public Set<Long> getAllCuboidIds() {
            return index.keySet();
        }

        public List<Long> getSpanningCuboid(long cuboidId) {
            TreeNode node = index.get(cuboidId);
            if (node == null) {
                throw new IllegalArgumentException("the cuboid:" + cuboidId + " is not exist in the tree");
            }

            List<Long> result = Lists.newArrayList();
            for (TreeNode child : node.children) {
                result.add(child.cuboidId);
            }
            return result;
        }

        public long findBestMatchCuboid(long cuboidId) {
            // exactly match
            if (isValid(cuboidId)) {
                return cuboidId;
            }

            return findBestParent(cuboidId).cuboidId;
        }

        public boolean isValid(long cuboidId) {
            return index.containsKey(cuboidId);
        }

        private int getCuboidCount(long cuboidId) {
            int r = 1;
            for (Long child : getSpanningCuboid(cuboidId)) {
                r += getCuboidCount(child);
            }
            return r;
        }

        public void print(PrintWriter out) {
            int dimensionCnt = Long.bitCount(root.cuboidId);
            doPrint(root, dimensionCnt, 0, out);
        }

        private void doPrint(TreeNode node, int dimensionCount, int depth, PrintWriter out) {
            printCuboid(node.cuboidId, dimensionCount, depth, out);

            for (TreeNode child : node.children) {
                doPrint(child, dimensionCount, depth + 1, out);
            }
        }

        private void printCuboid(long cuboidID, int dimensionCount, int depth, PrintWriter out) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < depth; i++) {
                sb.append("    ");
            }
            String cuboidName = Cuboid.getDisplayName(cuboidID, dimensionCount);
            sb.append("|---- Cuboid ").append(cuboidName).append("(" + cuboidID + ")");
            out.println(sb.toString());
        }

        private void setRoot(long basicCuboidId) {
            this.root = new TreeNode(basicCuboidId, 0);
            this.treeLevels = 0;
        }

        private void buildIndex() {
            LinkedList<TreeNode> queue = new LinkedList<>();
            queue.add(root);
            while (!queue.isEmpty()) {
                TreeNode node = queue.removeFirst();
                index.put(node.cuboidId, node);
                for (TreeNode child : node.children) {
                    queue.add(child);
                }
            }
        }

        private void addCuboid(long cuboidId) {
            TreeNode parent = findBestParent(cuboidId);
            if (parent != null && parent.cuboidId != cuboidId) {
                parent.addChild(cuboidId, parent.level);
                this.treeLevels = Math.max(this.treeLevels, parent.level + 1);
            }
        }

        private TreeNode findBestParent(long cuboidId) {
            TreeNode bestParent = doFindBestParent(cuboidId, root);
            if (bestParent == null) {
                throw new IllegalStateException("Cannot find the parent of the cuboid:" + cuboidId);
            }
            return bestParent;
        }

        private TreeNode doFindBestParent(long cuboidId, TreeNode parentCuboid) {
            if (!canDerive(cuboidId, parentCuboid.cuboidId)) {
                return null;
            }

            List<TreeNode> candidates = Lists.newArrayList();
            for (TreeNode childCuboid : parentCuboid.children) {
                TreeNode candidate = doFindBestParent(cuboidId, childCuboid);
                if (candidate != null) {
                    candidates.add(candidate);
                }
            }
            if (candidates.isEmpty()) {
                candidates.add(parentCuboid);
            }

            return Collections.min(candidates, new Comparator<TreeNode>() {
                @Override
                public int compare(TreeNode o1, TreeNode o2) {
                    return cuboidComparator.compare(o1.cuboidId, o2.cuboidId);
                }
            });
        }

        private boolean canDerive(long cuboidId, long parentCuboid) {
            return (cuboidId & ~parentCuboid) == 0;
        }
    }

    public static class TreeNode {
        @JsonProperty("cuboid_id")
        long cuboidId;
        @JsonIgnore
        int level;
        @JsonProperty("children")
        List<TreeNode> children = Lists.newArrayList();

        public long getCuboidId() {
            return cuboidId;
        }

        public int getLevel() {
            return level;
        }

        public List<TreeNode> getChildren() {
            return children;
        }

        TreeNode(long cuboidId, int level) {
            this.cuboidId = cuboidId;
            this.level = level;
        }

        void addChild(long childId, int parentlevel) {
            this.children.add(new TreeNode(childId, parentlevel + 1));
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (cuboidId ^ (cuboidId >>> 32));
            result = prime * result + level;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TreeNode other = (TreeNode) obj;
            if (cuboidId != other.cuboidId)
                return false;
            if (level != other.level)
                return false;
            return true;
        }
    }

    /**
     * Compare cuboid according to the cuboid data row count
     */
    public static class CuboidCostComparator implements Comparator<Long> {
        private Map<Long, Long> cuboidStatistics;

        public CuboidCostComparator(Map<Long, Long> cuboidStatistics) {
            Preconditions.checkArgument(cuboidStatistics != null,
                    "the input " + cuboidStatistics + " should not be null!!!");
            this.cuboidStatistics = cuboidStatistics;
        }

        @Override
        public int compare(Long cuboid1, Long cuboid2) {
            Long rowCnt1 = cuboidStatistics.get(cuboid1);
            Long rowCnt2 = cuboidStatistics.get(cuboid2);
            if (rowCnt2 == null || rowCnt1 == null) {
                return Cuboid.cuboidSelectComparator.compare(cuboid1, cuboid2);
            }
            return Long.compare(rowCnt1, rowCnt2);
        }
    }

    @Override
    public String getCuboidCacheKey() {
        return CubeInstance.class.getSimpleName() + "-" + cubeDesc.getName();
    }

    @Override
    public Set<Long> calculateCuboidsForAggGroup(AggregationGroup agg) {
        throw new UnsupportedOperationException();
    }
}
