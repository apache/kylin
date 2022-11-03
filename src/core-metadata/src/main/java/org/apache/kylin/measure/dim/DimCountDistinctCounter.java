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

import com.google.common.collect.Sets;

public class DimCountDistinctCounter {
    private final Set container;
    private final int MAX_CARD;

    public DimCountDistinctCounter() {
        container = Sets.newHashSet();
        MAX_CARD = KylinConfig.getInstanceFromEnv().getDimCountDistinctMaxCardinality();
    }

    public DimCountDistinctCounter(Set container, int MAX_CARD) {
        this.container = container;
        this.MAX_CARD = MAX_CARD;
    }

    public DimCountDistinctCounter(DimCountDistinctCounter other) {
        container = Sets.newHashSet(other.container);
        MAX_CARD = KylinConfig.getInstanceFromEnv().getDimCountDistinctMaxCardinality();
    }

    public void add(Object v) {
        if (container.size() >= MAX_CARD) {
            throw new RuntimeException("Cardinality of dimension exceeds the threshold: " + MAX_CARD);
        }
        container.add(v);
    }

    public void addAll(DimCountDistinctCounter counter) {
        if (container.size() + counter.container.size() >= MAX_CARD) {
            throw new RuntimeException("Cardinality of dimension exceeds the threshold: " + MAX_CARD);
        }
        container.addAll(counter.container);
    }

    public long result() {
        return container.size();
    }

    public int estimateSize() {
        return 20 * container.size();// 20 is just a guess
    }

    public Set getContainer() {
        return container;
    }

    public int getMAX_CARD() {
        return MAX_CARD;
    }
}
