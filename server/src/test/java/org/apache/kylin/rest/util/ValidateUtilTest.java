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

package org.apache.kylin.rest.util;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;

import static org.apache.kylin.metadata.MetadataConstants.TYPE_GROUP;
import static org.apache.kylin.metadata.MetadataConstants.TYPE_USER;

public class ValidateUtilTest extends ServiceTestBase {
    private final String PROJECT = "default";
    private final String NOT_EXISTS = "not_exists";
    private final String TABLE = "DEFAULT.TEST_KYLIN_FACT";

    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Test
    public void testCheckIdentifiersExists() throws IOException {
        validateUtil.checkIdentifiersExists("ADMIN", true);
        validateUtil.checkIdentifiersExists("ROLE_ADMIN", false);

        try {
            validateUtil.checkIdentifiersExists(NOT_EXISTS, true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Operation failed, user:not_exists not exists, please add first.", e.getMessage());
        }

        try {
            validateUtil.checkIdentifiersExists(NOT_EXISTS, false);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Operation failed, group:not_exists not exists, please add first.", e.getMessage());
        }
    }

    @Test
    public void testGetAndValidateIdentifiers() throws IOException {
        RootPersistentEntity ae = accessService.getAclEntity("ProjectInstance", "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");
        accessService.init(ae, AclPermission.ADMINISTRATION);
        accessService.grant(ae, AclPermission.ADMINISTRATION, accessService.getSid("u1", true));
        accessService.grant(ae, AclPermission.ADMINISTRATION, accessService.getSid("g1", false));

        Assert.assertEquals(Lists.newArrayList("ADMIN", "u1"),
                Lists.newArrayList(validateUtil.getAllIdentifiersInPrj(PROJECT, TYPE_USER)));
        Assert.assertEquals(Lists.newArrayList("g1"),
                Lists.newArrayList(validateUtil.getAllIdentifiersInPrj(PROJECT, TYPE_GROUP)));

        validateUtil.validateIdentifiers(PROJECT, "u1", TYPE_USER);
        try {
            validateUtil.validateIdentifiers(PROJECT, NOT_EXISTS, TYPE_USER);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Operation failed, user:not_exists not exists in project.", e.getMessage());
        }
    }

    @Test
    public void testValidateTable() throws IOException {
        validateUtil.validateTable(PROJECT, TABLE);
        try {
            validateUtil.validateTable(PROJECT, NOT_EXISTS);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Operation failed, table:not_exists not exists", e.getMessage());
        }
    }

    @Test
    public void testValidateColumns() throws IOException {
        validateUtil.validateColumn(PROJECT, TABLE, Lists.newArrayList("TRANS_ID"));
        try {
            validateUtil.validateColumn(PROJECT, TABLE, Lists.newArrayList(NOT_EXISTS));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Operation failed, column:not_exists not exists", e.getMessage());
        }
    }

    @Test
    public void testValidateArgs() {
        validateUtil.validateArgs(PROJECT);
        try {
            validateUtil.validateArgs("");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    public void testIsAlphanumericUnderscore() {
        Assert.assertTrue(ValidateUtil.isAlphanumericUnderscore("a"));
        Assert.assertTrue(ValidateUtil.isAlphanumericUnderscore("123"));
        Assert.assertTrue(ValidateUtil.isAlphanumericUnderscore("abc123"));
        Assert.assertTrue(ValidateUtil.isAlphanumericUnderscore("abc123_"));
        Assert.assertFalse(ValidateUtil.isAlphanumericUnderscore("abc@"));
        Assert.assertFalse(ValidateUtil.isAlphanumericUnderscore(""));
        Assert.assertFalse(ValidateUtil.isAlphanumericUnderscore(null));
    }
}
