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
package io.kyligence.kap.clickhouse;

public class ClickHouseConstants {

    private ClickHouseConstants() {
        throw new IllegalStateException("Utility class");
    }

    // root path => /project/clickhouse_plan
    public static final String RES_PATH_FMT = "/%s/%s_%s";

    public static final String STORAGE_NAME = "clickhouse";
    public static final String PLAN = "plan";
    public static final String DATA = "data";
    public static final String NODE_GROUP = "node_group";

    // property
    public static final String CONFIG_CLICKHOUSE_QUERY_CATALOG = "kylin.second-storage.jdbc-catalog";
}
