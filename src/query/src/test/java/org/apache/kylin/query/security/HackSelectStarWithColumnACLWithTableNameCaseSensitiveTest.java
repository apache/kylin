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

package org.apache.kylin.query.security;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@MetadataInfo
class HackSelectStarWithColumnACLWithTableNameCaseSensitiveTest {
    private static final String PROJECT = "default";
    private static final String SCHEMA = "DEFAULT";
    private static final HackSelectStarWithColumnACL TRANSFORMER = new HackSelectStarWithColumnACL();
    QueryContext current = QueryContext.current();

    @BeforeEach
    void setup() {
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
        getTestConfig().setProperty("kylin.source.name-case-sensitive-enabled", "true");
        prepareBasic();
        current.setAclInfo(new QueryContext.AclInfo("u1", Sets.newHashSet("g1"), false));
    }

    @AfterAll
    static void afterAll() {
        QueryContext.current().close();
    }

    @Test
    void testQuotedLowerCaseTableName() {
        //with kylin.source.name-case-sensitive-enabled = true
        // table identity is upper-case in metadata, quoted lower-case will throw exception
        {
            String sql = "select * from \"default\".TEST_KYLIN_FACT";
            Assert.assertThrows(KylinRuntimeException.class, () -> TRANSFORMER.convert(sql, PROJECT, SCHEMA));
        }
        // matched metadata
        {
            String sql = "select * from \"DEFAULT\".TEST_KYLIN_FACT";
            String convertedSql = TRANSFORMER.convert(sql, PROJECT, SCHEMA);
            String expected = "select * from ( select \"TEST_KYLIN_FACT\".\"order_id\" from"
                    + " \"DEFAULT\".\"TEST_KYLIN_FACT\") as \"TEST_KYLIN_FACT\"";
            Assert.assertNotNull(expected, convertedSql);
        }
    }

    private void prepareBasic() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), PROJECT);

        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column g1c1 = new AclTCR.Column();
        g1c1.add("ORDER_ID");
        g1cr1.setColumn(g1c1);
        g1t1.put("DEFAULT.TEST_KYLIN_FACT", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);
    }
}
