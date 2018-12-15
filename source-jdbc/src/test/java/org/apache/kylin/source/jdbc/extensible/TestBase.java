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
package org.apache.kylin.source.jdbc.extensible;

import java.sql.Connection;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.apache.kylin.source.H2Database;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestBase extends LocalFileMetadataTestCase {
    protected static Connection h2Conn = null;
    protected static H2Database h2Db = null;

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();

        JdbcConnector connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        h2Conn = connector.getConnection();

        h2Db = new H2Database(h2Conn, getTestConfig(), "default");
        h2Db.loadAllTables();
    }

    @AfterClass
    public static void after() throws Exception {
        h2Db.dropAll();
        DBUtils.closeQuietly(h2Conn);

        staticCleanupTestMetadata();
    }

    static class JdbcSourceAware implements ISourceAware {
        @Override
        public int getSourceType() {
            return JdbcSource.SOURCE_ID;
        }

        @Override
        public KylinConfig getConfig() {
            return getTestConfig();
        }
    }
}
