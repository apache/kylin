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

package org.apache.kylin.query.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.common.util.Pair;

public class KapRelUtil {

    private final static String CTX = "ctx=";

    private KapRelUtil() {
    }

    public static String getDigestWithoutRelNodeId(String digest, long layoutId, String modelId) {
        StringBuilder digestWithoutId = new StringBuilder();
        boolean isPointToId = false;
        for (char c : digest.toCharArray()) {
            if (isPointToId && !isCharNum(c)) {
                // end point to id
                isPointToId = false;
            }
            if (isPointToId) {
                continue;
            }
            if (c == '#') {
                // start point to id
                isPointToId = true;
            }
            digestWithoutId.append(c);
        }
        return replaceDigestCtxValueByLayoutIdAndModelId(digestWithoutId.toString(), layoutId, modelId);
    }

    public static String replaceDigestCtxValueByLayoutIdAndModelId(String digestId, long layoutId, String modelId) {
        if (layoutId <= 0 || "".equals(modelId)) {
            return digestId;
        }
        StringBuilder digestBuilder = new StringBuilder();
        char[] digestArray = digestId.toCharArray();
        char[] compareArray = CTX.toCharArray();
        int len = digestArray.length;
        for (int i = 0; i < len; i++) {
            char c1 = digestArray[i];
            if (c1 == compareArray[0] && (i + 3) < len && digestArray[i + 1] == compareArray[1]
                    && digestArray[i + 2] == compareArray[2] && digestArray[i + 3] == compareArray[3]) {
                digestBuilder.append(CTX);
                i = i + 2;
                while (digestArray[i + 1] != ',' && digestArray[i + 1] != ')') {
                    i = i + 1;
                }
                digestBuilder.append(modelId + "_" + layoutId);
            } else {
                digestBuilder.append(c1);
            }
        }
        return digestBuilder.toString();
    }

    private static boolean isCharNum(char c) {
        return c >= '0' && c <= '9';
    }

    public static RexNode isNotDistinctFrom(RelNode left, RelNode right, RexNode condition,
            List<Pair<Integer, Integer>> pairs, List<Boolean> filterNulls) {
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        RexNode rexNode = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys, filterNulls);
        for (int i = 0; i < leftKeys.size(); i++) {
            pairs.add(new Pair<>(leftKeys.get(i), rightKeys.get(i) + left.getRowType().getFieldCount()));
        }
        return rexNode;
    }
}
