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

package org.apache.kylin.rest.security;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
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
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

public class QueryWithTableACLTest extends LocalFileMetadataTestCase {
    private static final String PROJECT = "DEFAULT";
    private static final String ADMIN = "ADMIN";
    private static final String MODELER = "MODELER";
    private static final String STREAMING_TABLE = "DEFAULT.STREAMING_TABLE";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken(false, null));
        this.createTestMetadata();
    }

    @Test
    public void testNormalQuery() throws SQLException {
        QueryACLTestUtil.setUser(ADMIN);
        QueryACLTestUtil.mockQuery(PROJECT, "select * from STREAMING_TABLE");
    }

    @Test
    public void testFailQuery() throws SQLException, IOException {
        QueryACLTestUtil.setUser(MODELER);
        QueryACLTestUtil.mockQuery(PROJECT, "select * from STREAMING_TABLE");

        QueryACLTestUtil.setUser(ADMIN);
        TableACLManager.getInstance(KylinConfig.getInstanceFromEnv()).addTableACL(PROJECT, "ADMIN", STREAMING_TABLE, MetadataConstants.TYPE_USER);
        thrown.expectCause(CoreMatchers.isA(AccessDeniedException.class));
        thrown.expectMessage(CoreMatchers.containsString("Query failed.Access table:DEFAULT.STREAMING_TABLE denied"));
        QueryACLTestUtil.mockQuery(PROJECT, "select * from STREAMING_TABLE");
    }

    @Test
    public void testFailQueryWithCountStar() throws SQLException, IOException {
        QueryACLTestUtil.setUser(MODELER);
        QueryACLTestUtil.mockQuery(PROJECT, "select count(*) from STREAMING_TABLE");

        QueryACLTestUtil.setUser(ADMIN);
        TableACLManager.getInstance(KylinConfig.getInstanceFromEnv()).addTableACL(PROJECT, "ADMIN", STREAMING_TABLE, MetadataConstants.TYPE_USER);
        thrown.expectCause(CoreMatchers.isA(AccessDeniedException.class));
        thrown.expectMessage(CoreMatchers.containsString("Query failed.Access table:DEFAULT.STREAMING_TABLE denied"));
        QueryACLTestUtil.mockQuery(PROJECT, "select count(*) from STREAMING_TABLE");
    }

    @After
    public void after() throws Exception {
        SecurityContextHolder.getContext().setAuthentication(null);
        this.cleanupTestMetadata();
    }
}
