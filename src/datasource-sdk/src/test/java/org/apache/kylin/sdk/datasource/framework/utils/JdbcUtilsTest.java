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

package org.apache.kylin.sdk.datasource.framework.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.sdk.datasource.security.AbstractJdbcSourceConnectionValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JdbcUtilsTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testValidateUrlByWhiteList_schemeCheck() {
        overwriteSystemProp("kylin.source.jdbc.white-list.schemes", "mysql, postgresql");
        overwriteSystemProp("kylin.source.jdbc.white-list.mysql.validator-class",
                "org.apache.kylin.sdk.datasource.framework.utils.JdbcUtilsTest$MockJdbcSourceConnectionValidator");
        overwriteSystemProp("kylin.source.jdbc.white-list.postgresql.validator-class",
                "org.apache.kylin.sdk.datasource.framework.utils.JdbcUtilsTest$MockJdbcSourceConnectionValidator");

        // valid cases
        assertTrue(JdbcUtils.validateUrlByWhiteList("jdbc:mysql://localhost:3306/db"));
        assertTrue(JdbcUtils.validateUrlByWhiteList("jdbc:postgresql://localhost:5433/db"));

        // invalid cases
        assertFalse(JdbcUtils.validateUrlByWhiteList("xxx://localhost:3306/db"));
        assertFalse(JdbcUtils.validateUrlByWhiteList("jdbc:mongodb://localhost:1234/db"));
    }

    public static class MockJdbcSourceConnectionValidator extends AbstractJdbcSourceConnectionValidator {
        @Override
        public boolean isValid() {
            return true;
        }
    }
}
