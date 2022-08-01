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
package org.apache.kylin.query.util;

import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class SqlNodeExtractorTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetAllIdentifiers() throws Exception {
        String sql = "WITH a1 AS\n" + "  (SELECT * FROM t)\n" + "SELECT b.a1\n" + "FROM\n"
                + "  (WITH a2 AS (SELECT * FROM t) \n" + "    SELECT a2 FROM t2)\n" + "ORDER BY c_customer_id";
        SqlNodeExtractor sqlNodeExtractor = new SqlNodeExtractor();
        List<SqlIdentifier> identifiers = SqlNodeExtractor.getAllSqlIdentifier(sql);
        Assert.assertEquals(10, identifiers.size());
        val pos = SqlNodeExtractor.getIdentifierPos(sql);
        Assert.assertEquals(10, pos.size());
    }
}
