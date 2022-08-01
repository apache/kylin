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

package org.apache.kylin.metadata.model.util;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Lists;

public class MultiPartitionUtil {
    private MultiPartitionUtil() {
    }

    public static boolean isSameValue(String[] values1, String[] values2) {
        if (values1.length != values2.length) {
            return false;
        }
        for (int i = 0; i < values1.length; i++) {
            if (!values1[i].equals(values2[i])) {
                return false;
            }
        }
        return true;
    }

    private static Pair<List<String[]>, List<String[]>> partitionValues(List<String[]> originValues,
            List<String[]> addValues) {
        List<String[]> oldValues = cloneList(originValues);
        List<String[]> newValues = cloneList(addValues);

        List<String[]> duplicates = Lists.newArrayList();
        List<String[]> absents = Lists.newArrayList();
        for (String[] newValue : newValues) {
            boolean isSame = false;
            for (String[] oldValue : oldValues) {
                isSame = isSameValue(oldValue, newValue);
                if (isSame) {
                    duplicates.add(newValue);
                    break;
                }
            }
            if (!isSame) {
                absents.add(newValue);
            }
        }
        return Pair.newPair(duplicates, absents);
    }

    public static List<String[]> findDuplicateValues(List<String[]> originValues, List<String[]> addValues) {
        return partitionValues(originValues, addValues).getFirst();
    }

    public static List<String[]> findAbsentValues(List<String[]> originValues, List<String[]> addValues) {
        return partitionValues(originValues, addValues).getSecond();
    }

    public static List<String[]> findDiffValues(List<String[]> allValues, List<String[]> presentValues) {
        List<String[]> diffValues = Lists.newArrayList();
        for (String[] value : allValues) {
            if (presentValues.stream().noneMatch(v -> isSameValue(v, value))) {
                diffValues.add(value);
            }
        }
        return diffValues;
    }

    public static List<String[]> cloneList(List<String[]> list) {
        List<String[]> result = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(list)) {
            list.forEach(arr -> result.add(arr.clone()));
        }
        return result;
    }
}
