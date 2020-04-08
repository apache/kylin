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

package org.apache.kylin.engine.spark.metadata.cube;

import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class BitUtils {
    private static final Logger log = LoggerFactory.getLogger(BitUtils.class);

    public static List<Integer> tailor(List<Integer> complete, long cuboidId) {

        int bitCount = Long.bitCount(cuboidId);

        Integer[] ret = new Integer[bitCount];

        int next = 0;
        int size = complete.size();
        for (int i = 0; i < size; i++) {
            int shift = size - i - 1;
            if ((cuboidId & (1L << shift)) != 0) {
                ret[next++] = shift;
            }
        }

        return Arrays.asList(ret);
    }
}
