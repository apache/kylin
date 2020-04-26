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

package org.apache.kylin.engine.spark2;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.acl.TableACLManager;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.query.security.QueryACLTestUtil;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class QueryWithTableACLTest extends LocalWithSparkSessionTest {
    private static final String PROJECT = "DEFAULT";
    private static final String ADMIN = "ADMIN";
    private static final String MODELER = "MODELER";
    private static final String QUERY_TABLE = "DEFAULT.TEST_KYLIN_FACT";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws SchedulerException {
        super.setup();
        System.setProperty("spark.local", "true");
    }

    @Test
    public void testNormalQuery() throws SQLException {
        QueryACLTestUtil.setUser(ADMIN);
        QueryACLTestUtil.mockQuery(PROJECT, "select * from TEST_KYLIN_FACT");
    }

    @Test
    public void testFailQuery() throws SQLException, IOException {
        QueryACLTestUtil.setUser(MODELER);
        QueryACLTestUtil.mockQuery(PROJECT, "select * from TEST_KYLIN_FACT");

        QueryACLTestUtil.setUser(ADMIN);
        TableACLManager.getInstance(KylinConfig.getInstanceFromEnv()).addTableACL(PROJECT, "ADMIN", QUERY_TABLE, MetadataConstants.TYPE_USER);
        thrown.expectCause(CoreMatchers.isA(AccessDeniedException.class));
        thrown.expectMessage(CoreMatchers.containsString("Query failed.Access table:DEFAULT.TEST_KYLIN_FACT denied"));
        QueryACLTestUtil.mockQuery(PROJECT, "select * from TEST_KYLIN_FACT");
    }

    @Test
    public void testFailQueryWithCountStar() throws SQLException, IOException {
        QueryACLTestUtil.setUser(MODELER);
        QueryACLTestUtil.mockQuery(PROJECT, "select count(*) from TEST_KYLIN_FACT");

        QueryACLTestUtil.setUser(ADMIN);
        TableACLManager.getInstance(KylinConfig.getInstanceFromEnv()).addTableACL(PROJECT, "ADMIN", QUERY_TABLE, MetadataConstants.TYPE_USER);
        thrown.expectCause(CoreMatchers.isA(AccessDeniedException.class));
        thrown.expectMessage(CoreMatchers.containsString("Query failed.Access table:DEFAULT.TEST_KYLIN_FACT denied"));
        QueryACLTestUtil.mockQuery(PROJECT, "select count(*) from TEST_KYLIN_FACT");
    }

    @After
    public void after() {
        super.after();
    }
}
