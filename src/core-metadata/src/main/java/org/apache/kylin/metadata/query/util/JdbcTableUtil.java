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


package org.apache.kylin.metadata.query.util;

import java.sql.Connection;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcTableUtil {

    private JdbcTableUtil() {

    }

    public static boolean isTableExist(BasicDataSource dataSource, String tableName) {
        try (Connection connection = dataSource.getConnection()) {
            return JdbcUtil.isTableExists(connection, tableName);
        } catch (Exception e) {
            log.error("Fail to know if table {} exists", tableName, e);
            return true;
        }
    }

}
