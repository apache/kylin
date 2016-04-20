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

package org.apache.kylin.measure.dim;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class DimCountDistinctAggFunc {
    private static final Logger logger = LoggerFactory.getLogger(DimCountDistinctAggFunc.class);

    public static DimDistinctCounter init() {
        return null;
    }

    public static DimDistinctCounter initAdd(Object v) {
        DimDistinctCounter counter = new DimDistinctCounter();
        counter.add(v);
        return counter;
    }

    public static DimDistinctCounter add(DimDistinctCounter counter, Object v) {
        if (counter == null) {
            counter = new DimDistinctCounter();
        }
        counter.add(v);
        return counter;
    }

    public static DimDistinctCounter merge(DimDistinctCounter counter0, DimDistinctCounter counter1) {
        counter0.addAll(counter1);
        return counter0;
    }

    public static long result(DimDistinctCounter counter) {
        return counter == null ? 0L : counter.result();
    }

    public static class DimDistinctCounter {
        private final Set container;
        private final int MAX_LENGTH;

        public DimDistinctCounter() {
            container = Sets.newHashSet();
            MAX_LENGTH = KylinConfig.getInstanceFromEnv().getDimCountDistinctMaxCardinality();
        }

        public void add(Object v) {
            if (container.size() >= MAX_LENGTH) {
                throw new RuntimeException("Cardinality of dimension exceeds the threshold: " + MAX_LENGTH);
            }
            container.add(v);
        }

        public void addAll(DimDistinctCounter counter) {
            if (container.size() + counter.container.size() >= MAX_LENGTH) {
                throw new RuntimeException("Cardinality of dimension exceeds the threshold: " + MAX_LENGTH);
            }
            container.addAll(counter.container);
        }

        public long result() {
            return container.size();
        }
    }
}
