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
package org.apache.kylin.metadata.cube.planner.algorithm;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.StringUtil;
import org.junit.After;
import org.junit.Before;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ITAlgorithmTestBase {

    public final static Comparator<Long> CuboidSelectComparator = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return ComparisonChain.start().compare(Long.bitCount(o1), Long.bitCount(o2)).compare(o1, o2).result();
        }
    };

    private static class TreeNode implements Serializable {
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

    public static class CuboidTree implements Serializable {
        private int treeLevels;

        private TreeNode root;

        private Comparator<Long> cuboidComparator;

        private Map<Long, TreeNode> index = new HashMap<>();

        static CuboidTree createFromCuboids(List<Long> allCuboidIds) {
            return createFromCuboids(allCuboidIds, CuboidSelectComparator);
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
            String cuboidName = getDisplayName(cuboidID, dimensionCount);
            sb.append("|---- Cuboid ").append(cuboidName).append("(" + cuboidID + ")");
            out.println(sb.toString());
        }

        private String getDisplayName(long cuboidID, int dimensionCount) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dimensionCount; ++i) {
                if ((cuboidID & (1L << i)) == 0) {
                    sb.append('0');
                } else {
                    sb.append('1');
                }
            }
            return StringUtils.reverse(sb.toString());
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

    private static class CuboidCostComparator implements Comparator<Long>, Serializable {
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
                return CuboidSelectComparator.compare(cuboid1, cuboid2);
            }
            return Long.compare(rowCnt1, rowCnt2);
        }
    }

    public CuboidStats cuboidStats;

    private Set<Long> mandatoryCuboids;

    @Before
    public void setUp() throws Exception {

        mandatoryCuboids = Sets.newHashSet();
        mandatoryCuboids.add(3000L);
        mandatoryCuboids.add(1888L);
        mandatoryCuboids.add(88L);
        cuboidStats = new CuboidStats.Builder("test", 4095L, simulateCount(), simulateSpaceSize())
                .setMandatoryCuboids(mandatoryCuboids).setHitFrequencyMap(simulateHitFrequency())
                .setScanCountSourceMap(simulateScanCount()).build();
    }

    @After
    public void after() throws Exception {
    }

    /** better if closer to 1, worse if closer to 0*/
    public double getQueryCostRatio(CuboidStats cuboidStats, List<Long> recommendList) {
        CuboidTree cuboidTree = CuboidTree.createFromCuboids(recommendList,
                new CuboidCostComparator(cuboidStats.getStatistics()));
        double queryCostBest = 0;
        for (Long cuboidId : cuboidStats.getAllCuboidsForSelection()) {
            if (cuboidStats.getCuboidQueryCost(cuboidId) != null) {
                queryCostBest += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidCount(cuboidId);
                //                queryCostBest += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidQueryCost(cuboidId);
            }
        }

        double queryCost = 0;
        for (Long cuboidId : cuboidStats.getAllCuboidsForSelection()) {
            long matchCuboidId = cuboidTree.findBestMatchCuboid(cuboidId);
            if (cuboidStats.getCuboidQueryCost(matchCuboidId) != null) {
                queryCost += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidCount(matchCuboidId);
                //                queryCost += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidQueryCost(matchCuboidId);
            }
        }

        return queryCostBest / queryCost;
    }

    protected Map<Long, Long> simulateCount() {
        Map<Long, Long> countMap = Maps.newHashMap();
        BufferedReader br = null;

        try {

            String sCurrentLine;

            br = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/statistics.txt"),
                    StandardCharsets.UTF_8));

            while ((sCurrentLine = br.readLine()) != null) {
                String[] statPair = StringUtil.split(sCurrentLine, " ");
                countMap.put(Long.valueOf(statPair[0]), Long.valueOf(statPair[1]));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        return countMap;
    }

    protected Map<Long, Double> simulateSpaceSize() {
        Map<Long, Double> sizeMap = Maps.newHashMap();
        Map<Long, Long> countMap = simulateCount();
        for (Map.Entry<Long, Long> entry : countMap.entrySet()) {
            sizeMap.put(entry.getKey(), entry.getValue() * 1.0);
        }
        return sizeMap;
    }

    protected Map<Long, Long> simulateHitFrequency() {
        Map<Long, Long> hitFrequencyMap = Maps.newHashMap();

        hitFrequencyMap.put(4095L, 10L);
        hitFrequencyMap.put(3849L, 15L);
        hitFrequencyMap.put(3780L, 31L);

        hitFrequencyMap.put(3459L, 16L);
        hitFrequencyMap.put(3145L, 29L);

        hitFrequencyMap.put(2861L, 21L);
        hitFrequencyMap.put(2768L, 40L);

        hitFrequencyMap.put(1528L, 10L);
        hitFrequencyMap.put(1440L, 9L);
        hitFrequencyMap.put(1152L, 21L);

        hitFrequencyMap.put(256L, 23L);

        hitFrequencyMap.put(128L, 7L);
        hitFrequencyMap.put(272L, 8L);
        hitFrequencyMap.put(288L, 10L);
        hitFrequencyMap.put(384L, 2L);
        hitFrequencyMap.put(320L, 3L);
        hitFrequencyMap.put(432L, 5L);
        hitFrequencyMap.put(258L, 8L);
        hitFrequencyMap.put(336L, 10L);
        hitFrequencyMap.put(274L, 22L);
        hitFrequencyMap.put(488L, 41L);
        hitFrequencyMap.put(352L, 10L);

        hitFrequencyMap.put(16L, 1L);
        hitFrequencyMap.put(32L, 5L);
        hitFrequencyMap.put(34L, 1L);

        hitFrequencyMap.put(2L, 21L);

        return hitFrequencyMap;
    }

    protected Map<Long, Map<Long, Long>> simulateScanCount() {
        Map<Long, Map<Long, Long>> scanCountMap = Maps.newLinkedHashMap();
        scanCountMap.put(4094L, new HashMap<Long, Long>() {
            {
                put(4095L, 1833041L);
            }
        });
        scanCountMap.put(3849L, new HashMap<Long, Long>() {
            {
                put(3849L, 276711L);
            }
        });
        scanCountMap.put(3780L, new HashMap<Long, Long>() {
            {
                put(3780L, 129199L);
            }
        });
        scanCountMap.put(3459L, new HashMap<Long, Long>() {
            {
                put(3459L, 168109L);
            }
        });
        scanCountMap.put(3145L, new HashMap<Long, Long>() {
            {
                put(3145L, 299991L);
            }
        });
        scanCountMap.put(2861L, new HashMap<Long, Long>() {
            {
                put(2861L, 2121L);
            }
        });
        scanCountMap.put(2768L, new HashMap<Long, Long>() {
            {
                put(2768L, 40231L);
            }
        });
        scanCountMap.put(256L, new HashMap<Long, Long>() {
            {
                put(256L, 1L);
            }
        });
        scanCountMap.put(16L, new HashMap<Long, Long>() {
            {
                put(16L, 1L);
            }
        });
        scanCountMap.put(32L, new HashMap<Long, Long>() {
            {
                put(32L, 2L);
            }
        });
        scanCountMap.put(128L, new HashMap<Long, Long>() {
            {
                put(128L, 3L);
            }
        });
        scanCountMap.put(272L, new HashMap<Long, Long>() {
            {
                put(272L, 2L);
            }
        });
        scanCountMap.put(288L, new HashMap<Long, Long>() {
            {
                put(288L, 3L);
            }
        });
        scanCountMap.put(2L, new HashMap<Long, Long>() {
            {
                put(2L, 1L);
            }
        });
        scanCountMap.put(384L, new HashMap<Long, Long>() {
            {
                put(384L, 2L);
            }
        });
        scanCountMap.put(320L, new HashMap<Long, Long>() {
            {
                put(320L, 3L);
            }
        });
        scanCountMap.put(432L, new HashMap<Long, Long>() {
            {
                put(432L, 5L);
            }
        });
        scanCountMap.put(1152L, new HashMap<Long, Long>() {
            {
                put(1152L, 21L);
            }
        });
        scanCountMap.put(258L, new HashMap<Long, Long>() {
            {
                put(258L, 2L);
            }
        });
        scanCountMap.put(1440L, new HashMap<Long, Long>() {
            {
                put(1440L, 9L);
            }
        });
        scanCountMap.put(336L, new HashMap<Long, Long>() {
            {
                put(336L, 2L);
            }
        });
        scanCountMap.put(336L, new HashMap<Long, Long>() {
            {
                put(336L, 2L);
            }
        });
        scanCountMap.put(274L, new HashMap<Long, Long>() {
            {
                put(274L, 1L);
            }
        });
        scanCountMap.put(488L, new HashMap<Long, Long>() {
            {
                put(488L, 16L);
            }
        });
        scanCountMap.put(352L, new HashMap<Long, Long>() {
            {
                put(352L, 3L);
            }
        });
        scanCountMap.put(1528L, new HashMap<Long, Long>() {
            {
                put(1528L, 21L);
            }
        });
        scanCountMap.put(34L, new HashMap<Long, Long>() {
            {
                put(34L, 1L);
            }
        });

        return scanCountMap;
    }
}