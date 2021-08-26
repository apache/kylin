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

package org.apache.kylin.common;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

public class JDBCConnectionUtilsTest extends LocalFileMetadataTestCase {
    @BeforeClass
    public static void setupClass() throws SQLException {
        staticCreateTestMetadata();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.jdbc.url.allowed.properties", "database,useSSL,requireSSL,ssl,sslmode,integratedSecurity");
    }

    @Test
    public void testInvalidSchemaJdbcUrl() {
        String jdbcUrl = "jdbc:mysqld://fakehost:1433/database";
        try {
            JDBCConnectionUtils.checkUrl(jdbcUrl);
        } catch (Exception illegalException) {
            Assert.assertEquals("IllegalArgumentException: The data source schema : mysqld is not allowed. " +
                    "You can add the schema to the allowed schema list by kylin.jdbc.url.allowed.additional.schema " +
                    "and separate with commas.", ExceptionUtils.getRootCauseMessage(illegalException));
        }
    }

    @Test
    public void testInvalidHostMysqlJdbcUrl() {
        String jdbcUrl = "jdbc:mysql://fakehost&:1433/database";
        try {
            JDBCConnectionUtils.checkUrl(jdbcUrl);
        } catch (Exception illegalException) {
            Assert.assertEquals("IllegalArgumentException: Detected illegal character in fakehost& by "
                    + JDBCConnectionUtils.HOST_NAME_WHITE_LIST + ", jdbc url not allowed.", ExceptionUtils.getRootCauseMessage(illegalException));
        }
    }

    @Test
    public void testInvalidPortMysqlJdbcUrl() {
        String jdbcUrl = "jdbc:mysql://fakehost:1433F/database";
        try {
            JDBCConnectionUtils.checkUrl(jdbcUrl);
        } catch (Exception illegalException) {
            Assert.assertEquals("IllegalArgumentException: Detected illegal character in 1433F by "
                    + JDBCConnectionUtils.PORT_WHITE_LIST + ", jdbc url not allowed.", ExceptionUtils.getRootCauseMessage(illegalException));
        }
    }

    @Test
    public void testInvalidPortSqlserverJdbcUrl() {
        String jdbcUrl = "jdbc:sqlserver://fakehost:1433F;database=database";
        try {
            JDBCConnectionUtils.checkUrl(jdbcUrl);
        } catch (Exception illegalException) {
            Assert.assertEquals("IllegalArgumentException: Detected illegal character in 1433F by "
                    + JDBCConnectionUtils.PORT_WHITE_LIST + ", jdbc url not allowed.", ExceptionUtils.getRootCauseMessage(illegalException));
        }
    }

    @Test
    public void testInvalidPropertyMysqlJdbcUrl() {
        String jdbcUrl = "jdbc:mysql://fakehost:1433/database?allowLoadLocalInfile=true&autoDeserialize=false";
        try {
            JDBCConnectionUtils.checkUrl(jdbcUrl);
        } catch (Exception illegalException) {
            Assert.assertEquals("IllegalArgumentException: The property [allowLoadLocalInfile]" +
                    " is not in the allowed list database,useSSL,requireSSL,ssl,sslmode,integratedSecurity" +
                            ", you can add the property to the allowed properties list by kylin.jdbc.url.allowed.properties and separate with commas.",
                    ExceptionUtils.getRootCauseMessage(illegalException));
        }
    }

    @Test
    public void testInvalidDatabaseSqlServerJdbcUrl() {
        String jdbcUrl = "jdbc:sqlserver://fakehost:1433;database=database!;integratedSecurity=true;";
        try {
            JDBCConnectionUtils.checkUrl(jdbcUrl);
        } catch (Exception illegalException) {
            Assert.assertEquals("IllegalArgumentException: Detected illegal character in database! by "
                    + JDBCConnectionUtils.DATABASE_WHITE_LIST + ", jdbc url not allowed.", ExceptionUtils.getRootCauseMessage(illegalException));
        }
    }

    @Test
    public void testInvalidPropertySqlserverJdbcUrl() {
        String jdbcUrl = "jdbc:sqlserver://fakehost:1433;database=database;autoDeserialize=true";
        try {
            JDBCConnectionUtils.checkUrl(jdbcUrl);
        } catch (Exception illegalException) {
            Assert.assertEquals("IllegalArgumentException: The property [autoDeserialize]" +
                    " is not in the allowed list database,useSSL,requireSSL,ssl,sslmode,integratedSecurity" +
                    ", you can add the property to the allowed properties list by kylin.jdbc.url.allowed.properties and separate with commas.",
                    ExceptionUtils.getRootCauseMessage(illegalException));
        }
    }

    @Test
    public void testValidMysqlJdbcUrl() {
        String jdbcUrl = "jdbc:mysql://fakehost:1433/database_test?useSSL=true&requireSSL=true";
        Assert.assertEquals(jdbcUrl + "&" + JDBCConnectionUtils.APPEND_PARAMS, JDBCConnectionUtils.checkUrl(jdbcUrl));
    }

    @Test
    public void testValidSqlServerJdbcUrl() {
        String jdbcUrl = "jdbc:sqlserver://fakehost:1433;database=database_test;integratedSecurity=true;";
        Assert.assertEquals(jdbcUrl, JDBCConnectionUtils.checkUrl(jdbcUrl));
    }

    @Test
    public void testValidMsSqlServerJdbcUrl() {
        String jdbcUrl = "jdbc:microsoft:sqlserver://fakehost:1433;database=database_test;integratedSecurity=true;";
        Assert.assertEquals(jdbcUrl, JDBCConnectionUtils.checkUrl(jdbcUrl));
    }

    @Test
    public void testValidJtdsSqlServerJdbcUrl() {
        String jdbcUrl = "jdbc:jtds:sqlserver://fakehost:1433;database=database_test;integratedSecurity=true;";
        Assert.assertEquals(jdbcUrl, JDBCConnectionUtils.checkUrl(jdbcUrl));
    }
}
