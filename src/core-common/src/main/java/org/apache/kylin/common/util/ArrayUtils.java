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

package org.apache.kylin.common.util;

import java.util.List;

public class ArrayUtils {
    public static String[][] to2DArray(List<List<String>> list) {
        if (list == null) {
            return null;
        }

        String[][] result = new String[list.size()][];
        for (int i = 0; i < list.size(); i++) {
            List<String> child = list.get(i);
            if (child != null) {
                result[i] = child.toArray(new String[child.size()]);
            }
        }
        return result;
    }

    /**
     * Checks if the given subset is a prefix of the superset.
     *
     * A subset is considered a prefix of the superset if:
     * 1. The subset is shorter than or equal to the superset in length.
     * 2. All elements in the subset match the corresponding elements in the superset at the same positions.
     *
     * Examples:
     * - subset = ["a", "b"], superset = ["a", "b", "c"] -> returns true
     * - subset = ["a", "b"], superset = ["a", "c", "b"] -> returns false
     * - subset = ["a", "b", "c"], superset = ["a", "b"] -> returns false
     *
     * @param subset    The list to check if it is a prefix subset.
     * @param superset  The list to check against (the larger list).
     * @return          True if the subset is a prefix of the superset, otherwise false.
     */
    public static boolean isPrefixSubset(List<String> subset, List<String> superset) {
        if (subset.size() > superset.size()) {
            return false;
        }
        for (int i = 0; i < subset.size(); i++) {
            if (!subset.get(i).equals(superset.get(i))) {
                return false;
            }
        }
        return true;
    }
}
