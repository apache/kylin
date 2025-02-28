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

package org.apache.kylin.rec.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import lombok.extern.slf4j.Slf4j;

// Utility to generate the shortest readable table alias
@Slf4j
public class TableAliasGenerator {

    private TableAliasGenerator() {
    }

    private static final String UNKNOWN_SCHEMA = "N/A";
    private static final String KEY = "_KEY_";
    private static final String TO = "__TO__";

    public static TableAliasDict generateNewDict(String[] tableNames) {

        Map<String, List<String>> schemaMap = Maps.newLinkedHashMap();
        Set<String> unsortedNameSet = Sets.newHashSet(tableNames);

        unsortedNameSet.stream().sorted().filter(StringUtils::isNotEmpty) //
                .forEach(tableIdentity -> {
                    String[] splits = tableIdentity.split("\\.");
                    int lastIndex = splits.length - 1;
                    String table = splits[lastIndex];
                    String schema = lastIndex == 0 ? UNKNOWN_SCHEMA : splits[lastIndex - 1];
                    schemaMap.putIfAbsent(schema, Lists.newArrayList());
                    schemaMap.get(schema).add(table);
                });

        Map<String, String> schemaDict = schemaMap.size() > 1
                ? quickDict(schemaMap.keySet().toArray(new String[0]), true)
                : null;
        Map<String, String> dict = Maps.newLinkedHashMap();
        schemaMap.forEach((schema, tableList) -> {
            Map<String, String> tableDict = quickDict(tableList.toArray(new String[0]), false);
            tableDict.forEach((table, alias) -> {
                alias = schemaDict == null ? alias : schemaDict.get(schema) + "_" + alias;
                table = schema.equals(UNKNOWN_SCHEMA) ? table : schema + "." + table;
                dict.put(table, alias);
            });
        });

        return new TableAliasDict(dict);
    }

    public static TableAliasDict generateCommonDictForSpecificModel(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Map<String, TableDesc> allTablesMap = NTableMetadataManager.getInstance(config, project).getAllTablesMap();
        return generateNewDict(allTablesMap.keySet().toArray(new String[0]));
    }

    private static Map<String, String> quickDict(String[] sourceNames, boolean isSchema) {
        String prefix = isSchema ? "D" : "T";
        Map<String, String> dict = Maps.newHashMap();
        for (int i = 0; i < sourceNames.length; i++) {
            dict.put(sourceNames[i], prefix + i);
        }
        return dict;
    }

    public static class TableAliasDict {
        private final Map<String, String> alias2TblName = Maps.newHashMap();
        private final Map<String, String> tblName2Alias = Maps.newHashMap();

        public TableAliasDict(Map<String, String> dict) {
            dict.forEach((tableName, alias) -> {
                alias2TblName.putIfAbsent(alias, tableName);
                tblName2Alias.put(tableName, alias);
            });
        }

        public String getAlias(String tableName) {
            return tblName2Alias.get(tableName);
        }

        public String getTableName(String alias) {
            return alias2TblName.get(alias);
        }

        public String getHierarchyAliasFromJoins(JoinDesc[] joins) {
            if (ArrayUtils.isEmpty(joins)) {
                return "";
            }

            StringBuilder alias = new StringBuilder(getAlias(joins[0].getFKSide().getTableIdentity()));
            for (JoinDesc join : joins) {
                if (join.getPrimaryKeyColumns() == null
                        || join.getPrimaryKeyColumns().length == 0 && join.getNonEquiJoinCondition() == null) {
                    break;
                } else if (join.getNonEquiJoinCondition() != null) {
                    alias.append(KEY).append(join.getNonEquiJoinCondition().toString());
                    alias.append(TO).append(getAlias(join.getPKSide().getTableIdentity()));
                } else {
                    alias.append(KEY).append(Arrays
                            .toString(Arrays.stream(join.getForeignKeyColumns()).map(TblColRef::getName).toArray()));
                    alias.append(TO).append(getAlias(join.getPrimaryKeyColumns()[0].getTableRef().getTableIdentity()));
                    alias.append(KEY).append(Arrays
                            .toString(Arrays.stream(join.getPrimaryKeyColumns()).map(TblColRef::getName).toArray()));
                }
            }
            return alias.toString();
        }
    }
}
