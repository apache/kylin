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
package org.apache.kylin.source.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class JdbcTestBase extends NLocalFileMetadataTestCase {
    protected static JdbcConnector connector = null;
    protected static Connection h2Conn = null;
    protected static H2Database h2Db = null;

    @BeforeClass
    public static void setUp() throws SQLException {
        staticCreateTestMetadata();
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "testing");

        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        h2Conn = connector.getConnection();

        h2Db = new H2Database(h2Conn, getTestConfig(), "default");
        h2Db.loadAllTables();
    }

    @AfterClass
    public static void after() throws SQLException {
        h2Db.dropAll();
        DBUtils.closeQuietly(h2Conn);
        staticCleanupTestMetadata();
    }
}
