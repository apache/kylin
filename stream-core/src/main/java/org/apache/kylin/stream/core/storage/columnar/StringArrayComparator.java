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

package org.apache.kylin.stream.core.storage.columnar;

import java.util.Comparator;

public class StringArrayComparator implements Comparator<String[]> {
    public final static StringArrayComparator INSTANCE = new StringArrayComparator();

    @Override
    public int compare(String[] o1, String[] o2) {
        if (o1.length != o2.length) {
            return o1.length - o2.length;
        }
        int result = 0;
        for (int i = 0; i < o1.length; ++i) {
            if (o1[i] == null && o2[i] == null) {
                continue;
            } else if (o1[i] != null && o2[i] == null) {
                return 1;
            } else if (o1[i] == null && o2[i] != null) {
                return -1;
            } else {
                result = o1[i].compareTo(o2[i]);
                if (result == 0) {
                    continue;
                } else {
                    return result;
                }
            }
        }
        return result;
    }
}
