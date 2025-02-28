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
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JdbcUtilTest {

    private Connection connection;

    @Before
    public void setup() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa",
                null);
    }

    @Test
    public void testIsColumnExists() throws SQLException {
        String table = JdbcUtilTest.class.getSimpleName();
        connection.createStatement().execute("create table " + table + "(col1 int, col2 varchar)");
        Assert.assertTrue(JdbcUtil.isColumnExists(connection, table, "col1"));

        // case insensitive
        this.connection = DriverManager.getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa",
                null);
        Assert.assertTrue(JdbcUtil.isColumnExists(connection, table, "cOL1"));

        // not exists
        this.connection = DriverManager.getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa",
                null);
        Assert.assertFalse(JdbcUtil.isColumnExists(connection, table, "not_exists"));
    }

    @Test
    public void testIsIndexExists() throws SQLException {
        String table = "INDEXTEST";
        String index = "IX_1";
        connection.createStatement().execute("create table " + table + "(col1 int, col2 varchar)");
        connection.createStatement().execute("create index " + index + " on " + table + "(col1)");

        Connection connectionAutoRelease = DriverManager
                .getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", null);
        Assert.assertTrue(JdbcUtil.isIndexExists(connectionAutoRelease, table, index));
        Assert.assertTrue(connectionAutoRelease.isClosed());

        Connection connectionManuallyRelease = DriverManager
                .getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", null);
        Assert.assertTrue(JdbcUtil.isIndexExists(connectionManuallyRelease, table, index, false));
        Assert.assertFalse(connectionManuallyRelease.isClosed());

        Connection connectionAutoRelease1 = DriverManager
                .getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", null);
        Assert.assertTrue(JdbcUtil.isTableExists(connectionAutoRelease1, table));
        Assert.assertTrue(connectionAutoRelease1.isClosed());

        Connection connectionManuallyRelease1 = DriverManager
                .getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", null);
        Assert.assertTrue(JdbcUtil.isTableExists(connectionManuallyRelease1, table, false));
        Assert.assertFalse(connectionManuallyRelease1.isClosed());

    }

    @Test
    public void testRetry() throws Exception {
        Set set = new HashSet();
        int result = JdbcUtil.retry(() -> {
            boolean shouldThrowException = set.isEmpty();
            set.add(true);
            if (shouldThrowException) {
                throw new Exception("test");
            }
            return 1;
        });
        Assert.assertEquals(1, result);

    }
}
