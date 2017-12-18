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
package org.apache.kylin.source.jdbc.metadata;

import org.apache.kylin.source.hive.DBConnConf;
import org.apache.kylin.source.jdbc.JdbcDialect;

public abstract class JdbcMetadataFactory {
    public static IJdbcMetadata getJdbcMetadata(String dialect, final DBConnConf dbConnConf) {
        String jdbcDialect = (null == dialect) ? "" : dialect.toLowerCase();
        switch (jdbcDialect) {
        case (JdbcDialect.DIALECT_MSSQL):
            return new SQLServerJdbcMetadata(dbConnConf);
        case (JdbcDialect.DIALECT_MYSQL):
            return new MySQLJdbcMetadata(dbConnConf);
        default:
            return new DefaultJdbcMetadata(dbConnConf);
        }
    }
}
