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

package org.apache.kylin.engine.mr;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class LookupMaterializeContext {
    private DefaultChainedExecutable jobFlow;
    private Map<String, String> lookupSnapshotMap;

    public LookupMaterializeContext(DefaultChainedExecutable jobFlow) {
        this.jobFlow = jobFlow;
        this.lookupSnapshotMap = Maps.newHashMap();
    }

    public DefaultChainedExecutable getJobFlow() {
        return jobFlow;
    }

    /**
     * add snapshot path info into the context
     * @param lookupTable
     * @param snapshotPath
     */
    public void addLookupSnapshotPath(String lookupTable, String snapshotPath) {
        lookupSnapshotMap.put(lookupTable, snapshotPath);
    }

    /**
     *
     * @return string format of lookup snapshotPath info, it will return like: "lookup1=/path/uuid1,lookup2=/path/uuid2"
     *
     */
    public String getAllLookupSnapshotsInString() {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (Entry<String, String> lookupSnapshotEntry : lookupSnapshotMap.entrySet()) {
            if (!first) {
                result.append(",");
            }
            first = false;
            result.append(lookupSnapshotEntry.getKey());
            result.append("=");
            result.append(lookupSnapshotEntry.getValue());
        }
        return result.toString();
    }

    /**
     * parse the lookup snapshot string to lookup snapshot path map.
     * @param snapshotsString
     * @return
     */
    public static Map<String, String> parseLookupSnapshots(String snapshotsString) {
        Map<String, String> lookupSnapshotMap = Maps.newHashMap();
        String[] lookupSnapshotEntries = StringUtil.splitByComma(snapshotsString);
        for (String lookupSnapshotEntryStr : lookupSnapshotEntries) {
            String[] split = StringUtil.split(lookupSnapshotEntryStr, "=");
            lookupSnapshotMap.put(split[0], split[1]);
        }
        return lookupSnapshotMap;
    }
}
