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
package org.apache.spark.sql;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class DDLDesc {
    @JsonProperty("sql")
    private String sql;

    @JsonProperty("database")
    private String database;

    @JsonProperty("table")
    private String table;

    @JsonProperty("type")
    private DDLType type;

    public DDLDesc(String sql, String database, String table, DDLType type) {
        this.sql = sql;
        this.database = database;
        this.table = table;
        this.type = type;
    }

    public enum DDLType {
        CREATE_TABLE, CREATE_DATABASE, CREATE_VIEW, DROP_TABLE, DROP_DATABASE, ADD_PARTITION, NONE
    }
}
