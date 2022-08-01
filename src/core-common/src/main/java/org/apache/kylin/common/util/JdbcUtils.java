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
package org.apache.kylin.common.util;

import java.sql.Connection;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSourceFactory;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j

public class JdbcUtils {
    @SneakyThrows
    public static boolean checkConnectionParameter(String driver, String url, String username, String password) {
        Properties connProp = new Properties();
        connProp.put("driverClassName", driver);
        connProp.put("url", url);
        connProp.put("username", username);
        connProp.put("password", password);
        try (val dataSource = BasicDataSourceFactory.createDataSource(connProp);
                Connection conn = dataSource.getConnection()) {
            return true;
        } catch (Exception e) {
            log.debug("jdbc connect check failed", e);
            return false;
        }

    }
}
