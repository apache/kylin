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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.StringHelper;
import org.junit.After;
import org.junit.Before;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class AlgorithmTestBase {

    public final static Comparator<BigInteger> CuboidSelectComparator = new Comparator<BigInteger>() {
        @Override
        public int compare(BigInteger o1, BigInteger o2) {
            return ComparisonChain.start().compare(o1.bitCount(), o2.bitCount()).compare(o1, o2).result();
        }
    };

    private static class TreeNode implements Serializable {
        @JsonProperty("cuboid_id")
        BigInteger cuboidId;
        @JsonIgnore
        int level;
        @JsonProperty("children")
        List<TreeNode> children = Lists.newArrayList();

        public BigInteger getCuboidId() {
            return cuboidId;
        }

        public int getLevel() {
            return level;
        }

        public List<TreeNode> getChildren() {
            return children;
        }

        TreeNode(BigInteger cuboidId, int level) {
            this.cuboidId = cuboidId;
            this.level = level;
        }

        void addChild(BigInteger childId, int parentlevel) {
            this.children.add(new TreeNode(childId, parentlevel + 1));
        }

        @Override
        public int hashCode() {
            return Objects.hash(cuboidId, level);
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

        private Comparator<BigInteger> cuboidComparator;

        private Map<BigInteger, TreeNode> index = new HashMap<>();

        static CuboidTree createFromCuboids(List<BigInteger> allCuboidIds) {
            return createFromCuboids(allCuboidIds, CuboidSelectComparator);
        }

        public static CuboidTree createFromCuboids(List<BigInteger> allCuboidIds,
                                                   Comparator<BigInteger> cuboidComparator) {
            // sort the cuboid ids in descending order, so that don't need to adjust
            // the cuboid tree when adding cuboid id to the tree.
            Collections.sort(allCuboidIds, new Comparator<BigInteger>() {
                @Override
                public int compare(BigInteger o1, BigInteger o2) {
                    return o2.compareTo(o1);
                }
            });
            BigInteger basicCuboidId = allCuboidIds.get(0);
            CuboidTree cuboidTree = new CuboidTree(cuboidComparator);
            cuboidTree.setRoot(basicCuboidId);

            for (BigInteger cuboidId : allCuboidIds) {
                cuboidTree.addCuboid(cuboidId);
            }
            cuboidTree.buildIndex();
            return cuboidTree;
        }

        private CuboidTree(Comparator<BigInteger> cuboidComparator) {
            this.cuboidComparator = cuboidComparator;
        }

        public Set<BigInteger> getAllCuboidIds() {
            return index.keySet();
        }

        public List<BigInteger> getSpanningCuboid(BigInteger cuboidId) {
            TreeNode node = index.get(cuboidId);
            if (node == null) {
                throw new IllegalArgumentException("the cuboid:" + cuboidId + " is not exist in the tree");
            }

            List<BigInteger> result = Lists.newArrayList();
            for (TreeNode child : node.children) {
                result.add(child.cuboidId);
            }
            return result;
        }

        public BigInteger findBestMatchCuboid(BigInteger cuboidId) {
            // exactly match
            if (isValid(cuboidId)) {
                return cuboidId;
            }

            return findBestParent(cuboidId).cuboidId;
        }

        public boolean isValid(BigInteger cuboidId) {
            return index.containsKey(cuboidId);
        }

        private int getCuboidCount(BigInteger cuboidId) {
            int r = 1;
            for (BigInteger child : getSpanningCuboid(cuboidId)) {
                r += getCuboidCount(child);
            }
            return r;
        }

        public void print(PrintWriter out) {
            int dimensionCnt = root.cuboidId.bitCount();
            doPrint(root, dimensionCnt, 0, out);
        }

        private void doPrint(TreeNode node, int dimensionCount, int depth, PrintWriter out) {
            printCuboid(node.cuboidId, dimensionCount, depth, out);

            for (TreeNode child : node.children) {
                doPrint(child, dimensionCount, depth + 1, out);
            }
        }

        private void printCuboid(BigInteger cuboidID, int dimensionCount, int depth, PrintWriter out) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < depth; i++) {
                sb.append("    ");
            }
            String cuboidName = getDisplayName(cuboidID, dimensionCount);
            sb.append("|---- Cuboid ").append(cuboidName).append("(" + cuboidID + ")");
            out.println(sb.toString());
        }

        private String getDisplayName(BigInteger cuboidID, int dimensionCount) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dimensionCount; ++i) {

                if ((cuboidID.and((BigInteger.valueOf(1L << i))).equals(BigInteger.ZERO))) {
                    sb.append('0');
                } else {
                    sb.append('1');
                }
            }
            return StringUtils.reverse(sb.toString());
        }

        private void setRoot(BigInteger basicCuboidId) {
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

        private void addCuboid(BigInteger cuboidId) {
            TreeNode parent = findBestParent(cuboidId);
            if (parent != null && !parent.cuboidId.equals(cuboidId)) {
                parent.addChild(cuboidId, parent.level);
                this.treeLevels = Math.max(this.treeLevels, parent.level + 1);
            }
        }

        private TreeNode findBestParent(BigInteger cuboidId) {
            TreeNode bestParent = doFindBestParent(cuboidId, root);
            if (bestParent == null) {
                throw new IllegalStateException("Cannot find the parent of the cuboid:" + cuboidId);
            }
            return bestParent;
        }

        private TreeNode doFindBestParent(BigInteger cuboidId, TreeNode parentCuboid) {
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

        private boolean canDerive(BigInteger cuboidId, BigInteger parentCuboid) {
            return (cuboidId.and(parentCuboid.not())).equals(BigInteger.ZERO);
        }
    }

    private static class CuboidCostComparator implements Comparator<BigInteger>, Serializable {
        private Map<BigInteger, Long> cuboidStatistics;

        public CuboidCostComparator(Map<BigInteger, Long> cuboidStatistics) {
            Preconditions.checkArgument(cuboidStatistics != null,
                    "the input " + cuboidStatistics + " should not be null!!!");
            this.cuboidStatistics = cuboidStatistics;
        }

        @Override
        public int compare(BigInteger cuboid1, BigInteger cuboid2) {
            Long rowCnt1 = cuboidStatistics.get(cuboid1);
            Long rowCnt2 = cuboidStatistics.get(cuboid2);
            if (rowCnt2 == null || rowCnt1 == null) {
                return CuboidSelectComparator.compare(cuboid1, cuboid2);
            }
            return Long.compare(rowCnt1, rowCnt2);
        }
    }

    public CuboidStats cuboidStats;

    private Set<BigInteger> mandatoryCuboids;

    @Before
    public void setUp() throws Exception {

        mandatoryCuboids = Sets.newHashSet();
        mandatoryCuboids.add(BigInteger.valueOf(3000L));
        mandatoryCuboids.add(BigInteger.valueOf(1888L));
        mandatoryCuboids.add(BigInteger.valueOf(88L));
        cuboidStats = new CuboidStats.Builder("test", BigInteger.valueOf(4095L), BigInteger.valueOf(4095L),
                simulateCount(), simulateSpaceSize()).setMandatoryCuboids(mandatoryCuboids)
                .setHitFrequencyMap(simulateHitFrequency()).setScanCountSourceMap(simulateScanCount()).build();
    }

    @After
    public void after() throws Exception {
    }

    /** better if closer to 1, worse if closer to 0*/
    public double getQueryCostRatio(CuboidStats cuboidStats, List<BigInteger> recommendList) {
        CuboidTree cuboidTree = CuboidTree.createFromCuboids(recommendList,
                new CuboidCostComparator(cuboidStats.getStatistics()));
        double queryCostBest = 0;
        for (BigInteger cuboidId : cuboidStats.getAllCuboidsForSelection()) {
            if (cuboidStats.getCuboidQueryCost(cuboidId) != null) {
                queryCostBest += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidCount(cuboidId);
                //                queryCostBest += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidQueryCost(cuboidId);
            }
        }

        double queryCost = 0;
        for (BigInteger cuboidId : cuboidStats.getAllCuboidsForSelection()) {
            BigInteger matchCuboidId = cuboidTree.findBestMatchCuboid(cuboidId);
            if (cuboidStats.getCuboidQueryCost(matchCuboidId) != null) {
                queryCost += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidCount(matchCuboidId);
                //                queryCost += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidQueryCost(matchCuboidId);
            }
        }

        return queryCostBest / queryCost;
    }

    protected Map<BigInteger, Long> simulateCount() {
        Map<BigInteger, Long> countMap = Maps.newHashMap();
        BufferedReader br = null;

        try {

            String sCurrentLine;

            br = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/statistics.txt"),
                    StandardCharsets.UTF_8));

            while ((sCurrentLine = br.readLine()) != null) {
                String[] statPair = StringHelper.split(sCurrentLine, " ");
                countMap.put(BigInteger.valueOf(Long.valueOf(statPair[0])), Long.valueOf(statPair[1]));
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

    protected Map<BigInteger, Double> simulateSpaceSize() {
        Map<BigInteger, Double> sizeMap = Maps.newHashMap();
        Map<BigInteger, Long> countMap = simulateCount();
        for (Map.Entry<BigInteger, Long> entry : countMap.entrySet()) {
            sizeMap.put(entry.getKey(), entry.getValue() * 1.0);
        }
        return sizeMap;
    }

    protected Map<BigInteger, Long> simulateHitFrequency() {
        Map<BigInteger, Long> hitFrequencyMap = Maps.newHashMap();

        hitFrequencyMap.put(BigInteger.valueOf(4095L), 10L);
        hitFrequencyMap.put(BigInteger.valueOf(3849L), 15L);
        hitFrequencyMap.put(BigInteger.valueOf(3780L), 31L);

        hitFrequencyMap.put(BigInteger.valueOf(3459L), 16L);
        hitFrequencyMap.put(BigInteger.valueOf(3145L), 29L);

        hitFrequencyMap.put(BigInteger.valueOf(2861L), 21L);
        hitFrequencyMap.put(BigInteger.valueOf(2768L), 40L);

        hitFrequencyMap.put(BigInteger.valueOf(1528L), 10L);
        hitFrequencyMap.put(BigInteger.valueOf(1440L), 9L);
        hitFrequencyMap.put(BigInteger.valueOf(1152L), 21L);

        hitFrequencyMap.put(BigInteger.valueOf(256L), 23L);

        hitFrequencyMap.put(BigInteger.valueOf(128L), 7L);
        hitFrequencyMap.put(BigInteger.valueOf(272L), 8L);
        hitFrequencyMap.put(BigInteger.valueOf(288L), 10L);
        hitFrequencyMap.put(BigInteger.valueOf(384L), 2L);
        hitFrequencyMap.put(BigInteger.valueOf(320L), 3L);
        hitFrequencyMap.put(BigInteger.valueOf(432L), 5L);
        hitFrequencyMap.put(BigInteger.valueOf(258L), 8L);
        hitFrequencyMap.put(BigInteger.valueOf(336L), 10L);
        hitFrequencyMap.put(BigInteger.valueOf(274L), 22L);
        hitFrequencyMap.put(BigInteger.valueOf(488L), 41L);
        hitFrequencyMap.put(BigInteger.valueOf(352L), 10L);

        hitFrequencyMap.put(BigInteger.valueOf(16L), 1L);
        hitFrequencyMap.put(BigInteger.valueOf(32L), 5L);
        hitFrequencyMap.put(BigInteger.valueOf(34L), 1L);

        hitFrequencyMap.put(BigInteger.valueOf(2L), 21L);

        return hitFrequencyMap;
    }

    protected Map<BigInteger, Map<BigInteger, Long>> simulateScanCount() {
        Map<BigInteger, Map<BigInteger, Long>> scanCountMap = Maps.newLinkedHashMap();
        scanCountMap.put(BigInteger.valueOf(4094L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(4095L), 1833041L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(3849L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(3849L), 276711L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(3780L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(3780L), 129199L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(3459L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(3459L), 168109L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(3145L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(3145L), 299991L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(2861L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(2861L), 2121L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(2768L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(2768L), 40231L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(256L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(256L), 1L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(16L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(16L), 1L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(32L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(32L), 2L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(128L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(128L), 3L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(272L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(272L), 2L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(288L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(288L), 3L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(2L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(2L), 1L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(384L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(384L), 2L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(320L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(320L), 3L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(432L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(432L), 5L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(1152L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(1152L), 21L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(258L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(258L), 2L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(1440L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(1440L), 9L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(336L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(336L), 2L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(336L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(336L), 2L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(274L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(274L), 1L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(488L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(488L), 16L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(352L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(352L), 3L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(1528L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(1528L), 21L);
            }
        });
        scanCountMap.put(BigInteger.valueOf(34L), new HashMap<BigInteger, Long>() {
            {
                put(BigInteger.valueOf(34L), 1L);
            }
        });

        return scanCountMap;
    }
}
