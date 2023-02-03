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
package org.apache.kylin.common.persistence.metadata.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JdbcUtilTest {

    private Connection connection;

    @Before
    public void setup() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1", "sa",
                null);
    }

    @Test
    public void testIsColumnExists() throws SQLException {
        String table = JdbcUtilTest.class.getSimpleName();
        connection.createStatement().execute("create table " + table + "(col1 int, col2 varchar)");
        Assert.assertTrue(JdbcUtil.isColumnExists(connection, table, "col1"));

        // case insensitive
        this.connection = DriverManager.getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1", "sa",
                null);
        Assert.assertTrue(JdbcUtil.isColumnExists(connection, table, "cOL1"));

        // not exists
        this.connection = DriverManager.getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1", "sa",
                null);
        Assert.assertFalse(JdbcUtil.isColumnExists(connection, table, "not_exists"));
    }
}
