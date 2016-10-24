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

package org.apache.kylin.query.routing.rules;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.RoutingRule;

import com.google.common.collect.Sets;

/**
 * for IT use, exclude some cubes 
 */
public class RemoveBlackoutRealizationsRule extends RoutingRule {
    public static Set<String> blackList = Sets.newHashSet();
    public static Set<String> whiteList = Sets.newHashSet();

    public static boolean accept(IRealization real) {
        if (blackList.contains(real.getCanonicalName()))
            return false;
        if (!whiteList.isEmpty() && !whiteList.contains(real.getCanonicalName()))
            return false;
        
        return true;
    }
    
    @Override
    public void apply(List<Candidate> candidates) {
        for (Iterator<Candidate> iterator = candidates.iterator(); iterator.hasNext();) {
            Candidate candidate = iterator.next();

            if (!accept(candidate.getRealization())) {
                iterator.remove();
            }
        }
    }

}
