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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;

public class DependentColumnInfo {

    // map<col -> map<depCol, depColInfo>>
    Map<String, Map<String, DependentColumn>> infos = new HashMap<>();

    public boolean needMask() {
        return !infos.isEmpty();
    }

    public void add(String dbName, String tableName, Collection<DependentColumn> columnInfos) {
        for (DependentColumn info : columnInfos) {
            infos.putIfAbsent(dbName + "." + tableName + "." + info.getColumn(), new HashMap<>());

            DependentColumn dependentColumn = infos.get(dbName + "." + tableName + "." + info.getColumn())
                    .get(info.getDependentColumnIdentity());
            if (dependentColumn != null) {
                infos.get(dbName + "." + tableName + "." + info.getColumn()).put(info.getDependentColumnIdentity(),
                        dependentColumn.merge(info));
            } else {
                infos.get(dbName + "." + tableName + "." + info.getColumn()).put(info.getDependentColumnIdentity(),
                        info);
            }
        }
    }

    public void validate() {
        infos.values().stream().flatMap(map -> map.values().stream()).forEach(col -> {
            if (!get(col.getDependentColumnIdentity()).isEmpty()) {
                throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getNotSupportNestedDependentCol());
            }
        });
    }

    public Collection<DependentColumn> get(String columnIdentity) {
        return infos.getOrDefault(columnIdentity, new HashMap<>()).values();
    }

}
