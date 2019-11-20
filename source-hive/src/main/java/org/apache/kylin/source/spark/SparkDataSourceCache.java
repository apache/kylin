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

package org.apache.kylin.source.spark;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Lists;

/**
 * Cache for storing table names and their corresponding spark datasource information, thread safe.
 */
public class SparkDataSourceCache {
    private static Map<String, SparkDataSourceDesc> table2DataSource = new ConcurrentHashMap<>();

    public static void add(String tableName, SparkDataSourceDesc dataSourceDesc) {
        table2DataSource.put(tableName.toLowerCase(), dataSourceDesc);
    }

    public static SparkDataSourceDesc get(String tableName) {
        return table2DataSource.get(tableName.toLowerCase());
    }

    public static List<String> getTables() {
        List<String> tables = Lists.newArrayList();
        for (String table: table2DataSource.keySet()) {
            tables.add(table);
        }
        return tables;
    }
}
