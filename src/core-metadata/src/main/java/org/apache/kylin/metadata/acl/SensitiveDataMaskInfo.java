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

package org.apache.kylin.metadata.acl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SensitiveDataMaskInfo {

    private Map<String, Map<String, SensitiveDataMask>> infos = new HashMap<>();

    public boolean hasMask() {
        return !infos.isEmpty();
    }

    public void addMasks(String dbName, String tableName, Collection<SensitiveDataMask> masks) {
        for (SensitiveDataMask mask : masks) {
            infos.putIfAbsent(dbName + "." + tableName, new HashMap<>());
            SensitiveDataMask originalMask = infos.get(dbName + "." + tableName).get(mask.column);
            if (originalMask != null) {
                infos.get(dbName + "." + tableName).put(mask.column,
                        new SensitiveDataMask(mask.column, mask.getType().merge(originalMask.getType())));
            } else {
                infos.get(dbName + "." + tableName).put(mask.column, mask);
            }
        }
    }

    public SensitiveDataMask getMask(String dbName, String tableName, String columnName) {
        if (infos.containsKey(dbName + "." + tableName)) {
            return infos.get(dbName + "." + tableName).get(columnName);
        }
        return null;
    }

}
