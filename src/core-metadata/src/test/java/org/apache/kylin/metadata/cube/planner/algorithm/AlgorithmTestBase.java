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

public class AlgorithmTestBase {

    public final static Comparator<BigInteger> LayoutSelectComparator = (o1, o2) -> ComparisonChain.start()
            .compare(o1.bitCount(), o2.bitCount()).compare(o1, o2).result();

    private static class TreeNode implements Serializable {
        @JsonProperty("layout_id")
        BigInteger layoutId;
        @JsonIgnore
        int level;
        @JsonProperty("children")
        List<TreeNode> children = Lists.newArrayList();

        public BigInteger getLayoutId() {
            return layoutId;
        }

        public int getLevel() {
            return level;
        }

        public List<TreeNode> getChildren() {
            return children;
        }

        TreeNode(BigInteger layoutId, int level) {
            this.layoutId = layoutId;
            this.level = level;
        }

        void addChild(BigInteger childId, int parentlevel) {
            this.children.add(new TreeNode(childId, parentlevel + 1));
        }

        @Override
        public int hashCode() {
            return Objects.hash(layoutId, level);
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
            if (!Objects.equals(layoutId, other.layoutId))
                return false;
            return level == other.level;
        }
    }

    public static class LayoutTree implements Serializable {
        private int treeLevels;

        private TreeNode root;

        private final Comparator<BigInteger> layoutComparator;

        private final Map<BigInteger, TreeNode> index = new HashMap<>();

        static LayoutTree createFromLayouts(List<BigInteger> allLayoutIds) {
            return createFromLayouts(allLayoutIds, LayoutSelectComparator);
        }

        public static LayoutTree createFromLayouts(List<BigInteger> allLayoutIds,
                Comparator<BigInteger> layoutComparator) {
            // sort the layout ids in descending order, so that don't need to adjust
            // the layout tree when adding layout id to the tree.
            allLayoutIds.sort(Comparator.reverseOrder());
            BigInteger basicLayoutId = allLayoutIds.get(0);
            LayoutTree layoutTree = new LayoutTree(layoutComparator);
            layoutTree.setRoot(basicLayoutId);

            for (BigInteger layoutId : allLayoutIds) {
                layoutTree.addLayout(layoutId);
            }
            layoutTree.buildIndex();
            return layoutTree;
        }

        private LayoutTree(Comparator<BigInteger> layoutComparator) {
            this.layoutComparator = layoutComparator;
        }

        public Set<BigInteger> getAllLayoutIds() {
            return index.keySet();
        }

        public List<BigInteger> getSpanningLayout(BigInteger layoutId) {
            TreeNode node = index.get(layoutId);
            if (node == null) {
                throw new IllegalArgumentException("the layout:" + layoutId + " is not exist in the tree");
            }

            List<BigInteger> result = Lists.newArrayList();
            for (TreeNode child : node.children) {
                result.add(child.layoutId);
            }
            return result;
        }

        public BigInteger findBestMatchLayout(BigInteger layoutId) {
            // exactly match
            if (isValid(layoutId)) {
                return layoutId;
            }

            return findBestParent(layoutId).layoutId;
        }

        public boolean isValid(BigInteger layoutId) {
            return index.containsKey(layoutId);
        }

        private int getLayoutCount(BigInteger layoutId) {
            int r = 1;
            for (BigInteger child : getSpanningLayout(layoutId)) {
                r += getLayoutCount(child);
            }
            return r;
        }

        public void print(PrintWriter out) {
            int dimensionCnt = root.layoutId.bitCount();
            doPrint(root, dimensionCnt, 0, out);
        }

        private void doPrint(TreeNode node, int dimensionCount, int depth, PrintWriter out) {
            printLayout(node.layoutId, dimensionCount, depth, out);

            for (TreeNode child : node.children) {
                doPrint(child, dimensionCount, depth + 1, out);
            }
        }

        private void printLayout(BigInteger layoutId, int dimensionCount, int depth, PrintWriter out) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < depth; i++) {
                sb.append("    ");
            }
            String layoutName = getDisplayName(layoutId, dimensionCount);
            sb.append("|---- Layout ").append(layoutName).append("(").append(layoutId).append(")");
            out.println(sb);
        }

        private String getDisplayName(BigInteger layoutId, int dimensionCount) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dimensionCount; ++i) {

                if ((layoutId.and((BigInteger.valueOf(1L << i))).equals(BigInteger.ZERO))) {
                    sb.append('0');
                } else {
                    sb.append('1');
                }
            }
            return StringUtils.reverse(sb.toString());
        }

        private void setRoot(BigInteger basicLayoutId) {
            this.root = new TreeNode(basicLayoutId, 0);
            this.treeLevels = 0;
        }

        private void buildIndex() {
            LinkedList<TreeNode> queue = new LinkedList<>();
            queue.add(root);
            while (!queue.isEmpty()) {
                TreeNode node = queue.removeFirst();
                index.put(node.layoutId, node);
                queue.addAll(node.children);
            }
        }

        private void addLayout(BigInteger layoutId) {
            TreeNode parent = findBestParent(layoutId);
            if (!parent.layoutId.equals(layoutId)) {
                parent.addChild(layoutId, parent.level);
                this.treeLevels = Math.max(this.treeLevels, parent.level + 1);
            }
        }

        private TreeNode findBestParent(BigInteger layoutId) {
            TreeNode bestParent = doFindBestParent(layoutId, root);
            if (bestParent == null) {
                throw new IllegalStateException("Cannot find the parent of the layout:" + layoutId);
            }
            return bestParent;
        }

        private TreeNode doFindBestParent(BigInteger layoutId, TreeNode parentLayout) {
            if (!canDerive(layoutId, parentLayout.layoutId)) {
                return null;
            }

            List<TreeNode> candidates = Lists.newArrayList();
            for (TreeNode childLayout : parentLayout.children) {
                TreeNode candidate = doFindBestParent(layoutId, childLayout);
                if (candidate != null) {
                    candidates.add(candidate);
                }
            }
            if (candidates.isEmpty()) {
                candidates.add(parentLayout);
            }

            return Collections.min(candidates, new Comparator<TreeNode>() {
                @Override
                public int compare(TreeNode o1, TreeNode o2) {
                    return layoutComparator.compare(o1.layoutId, o2.layoutId);
                }
            });
        }

        private boolean canDerive(BigInteger layoutId, BigInteger parentLayout) {
            return (layoutId.and(parentLayout.not())).equals(BigInteger.ZERO);
        }
    }

    private static class LayoutCostComparator implements Comparator<BigInteger>, Serializable {
        private final Map<BigInteger, Long> layoutStatistics;

        public LayoutCostComparator(Map<BigInteger, Long> layoutStatistics) {
            Preconditions.checkArgument(layoutStatistics != null,
                    "the input " + layoutStatistics + " should not be null!!!");
            this.layoutStatistics = layoutStatistics;
        }

        @Override
        public int compare(BigInteger layout1, BigInteger layout2) {
            Long rowCnt1 = layoutStatistics.get(layout1);
            Long rowCnt2 = layoutStatistics.get(layout2);
            if (rowCnt2 == null || rowCnt1 == null) {
                return LayoutSelectComparator.compare(layout1, layout2);
            }
            return Long.compare(rowCnt1, rowCnt2);
        }
    }

    public LayoutStats layoutStats;

    @Before
    public void setUp() throws Exception {

        Set<BigInteger> mandatoryLayouts = Sets.newHashSet();
        mandatoryLayouts.add(BigInteger.valueOf(3000L));
        mandatoryLayouts.add(BigInteger.valueOf(1888L));
        mandatoryLayouts.add(BigInteger.valueOf(88L));
        layoutStats = new LayoutStats.Builder("test", BigInteger.valueOf(4095L), BigInteger.valueOf(4095L),
                simulateCount(), simulateSpaceSize()).setMandatoryLayouts(mandatoryLayouts)
                        .setHitFrequencyMap(simulateHitFrequency()).setScanCountSourceMap(simulateScanCount()).build();
    }

    @After
    public void after() throws Exception {
    }

    /** better if closer to 1, worse if closer to 0*/
    public double getQueryCostRatio(LayoutStats layoutStats, List<BigInteger> recommendList) {
        LayoutTree layoutTree = LayoutTree.createFromLayouts(recommendList,
                new LayoutCostComparator(layoutStats.getStatistics()));
        double queryCostBest = 0;
        for (BigInteger layoutId : layoutStats.getAllLayoutsForSelection()) {
            if (layoutStats.getLayoutQueryCost(layoutId) != null) {
                queryCostBest += layoutStats.getLayoutHitProb(layoutId) * layoutStats.getLayoutCount(layoutId);
                // queryCostBest += layoutStats.getLayoutHitProb(layoutId) * layoutStats.getLayoutQueryCost(layoutId);
            }
        }

        double queryCost = 0;
        for (BigInteger layoutId : layoutStats.getAllLayoutsForSelection()) {
            BigInteger matchLayoutId = layoutTree.findBestMatchLayout(layoutId);
            if (layoutStats.getLayoutQueryCost(matchLayoutId) != null) {
                queryCost += layoutStats.getLayoutHitProb(layoutId) * layoutStats.getLayoutCount(matchLayoutId);
                // queryCost += layoutStats.getLayoutHitProb(layoutId) * layoutStats.getLayoutQueryCost(matchLayoutId);
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
                String[] statPair = StringUtil.split(sCurrentLine, " ");
                countMap.put(BigInteger.valueOf(Long.parseLong(statPair[0])), Long.valueOf(statPair[1]));
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