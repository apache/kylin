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

package org.apache.kylin.query.routing;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;

import com.google.common.collect.Maps;

public class Candidate {
    public static final CandidateComparator COMPARATOR = new CandidateComparator();

    static Map<RealizationType, Integer> DEFAULT_PRIORITIES = Maps.newHashMap();
    static Map<RealizationType, Integer> PRIORITIES = DEFAULT_PRIORITIES;

    static {
        DEFAULT_PRIORITIES.put(RealizationType.HYBRID, 0);
        DEFAULT_PRIORITIES.put(RealizationType.CUBE, 1);
    }

    /** for test only */
    Candidate() {
    }

    /** for test only */
    public static void setPriorities(Map<RealizationType, Integer> priorities) {
        PRIORITIES = Collections.unmodifiableMap(priorities);
    }

    /** for test only */
    public static void restorePriorities() {
        PRIORITIES = Collections.unmodifiableMap(DEFAULT_PRIORITIES);
    }

    // ============================================================================

    IRealization realization;
    SQLDigest sqlDigest;
    int priority;
    CapabilityResult capability;

    public Candidate(IRealization realization, SQLDigest sqlDigest) {
        this.realization = realization;
        this.sqlDigest = sqlDigest;
        this.priority = PRIORITIES.get(realization.getType());
    }

    public IRealization getRealization() {
        return realization;
    }

    public SQLDigest getSqlDigest() {
        return sqlDigest;
    }

    public int getPriority() {
        return priority;
    }

    public CapabilityResult getCapability() {
        return capability;
    }

    public void setCapability(CapabilityResult capability) {
        this.capability = capability;
    }

    @Override
    public String toString() {
        return realization.toString();
    }

    public static class CandidateComparator implements Comparator<Candidate> {

        @Override
        public int compare(Candidate c1, Candidate c2) {
            IRealization real1 = c1.getRealization();
            IRealization real2 = c2.getRealization();

            if (QueryContextFacade.current().getCubePriorities().length > 0) {

                Map<String, Integer> priorities = new HashMap<>();
                for (int i = 0; i < QueryContextFacade.current().getCubePriorities().length; i++) {
                    priorities.put(QueryContextFacade.current().getCubePriorities()[i], i);
                }

                int comp = priorities.getOrDefault(real1.getName(), Integer.MAX_VALUE)
                        - priorities.getOrDefault(real2.getName(), Integer.MAX_VALUE);
                if (comp != 0) {
                    return comp;
                }
            }

            int comp = real1.getCost() - real2.getCost();
            if (comp != 0) {
                return comp;
            }

            comp = Double.compare(c1.capability.cost, c2.capability.cost);
            if (comp != 0) {
                return comp;
            }

            return 0;
        }
    }
}
