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

package org.apache.kylin.metadata.cube.cuboid;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealizationCandidate;

import lombok.Getter;
import lombok.Setter;

/**
 * Both Snapshot and InternalTable can be used as LookupCandidate.
 */
@Getter
public class NLookupCandidate implements IRealizationCandidate {

    private final String table;
    private final Policy policy;
    @Setter
    private CapabilityResult capabilityResult;

    public NLookupCandidate(String table, Policy policy) {
        this.table = table;
        this.policy = policy;
    }

    @Override
    public double getCost() {
        return 0d;
    }

    public enum Policy {
        SNAPSHOT,

        /** This policy prioritizes matching the aggregate index first;
         * if that fails, it defaults to the snapshot. */
        AGG_THEN_SNAPSHOT,

        INTERNAL_TABLE,

        /** This policy prioritizes matching the aggregate index first;
         * if that fails, it defaults to the internal table. */
        AGG_THEN_INTERNAL_TABLE,

        NONE
    }

    public static Policy getDerivedPolicy(KylinConfig config) {
        return config.isInternalTableEnabled() ? Policy.INTERNAL_TABLE : Policy.SNAPSHOT;
    }
}
