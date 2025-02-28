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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExternalAclProviderTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testConvertToExternalPermissionWithMask() {
        String value = ExternalAclProvider.convertToExternalPermission(0);
        Assert.assertEquals("EMPTY", value);

        value = ExternalAclProvider.convertToExternalPermission(1);
        Assert.assertEquals("QUERY", value);

        value = ExternalAclProvider.convertToExternalPermission(64);
        Assert.assertEquals("OPERATION", value);

        value = ExternalAclProvider.convertToExternalPermission(32);
        Assert.assertEquals("MANAGEMENT", value);

        value = ExternalAclProvider.convertToExternalPermission(16);
        Assert.assertEquals("ADMIN", value);

        thrown.expectMessage("Invalid permission state: -1");
        ExternalAclProvider.convertToExternalPermission(-1);
    }

    @Test
    public void testConvertToExternalPermissionWithPermission() {
        String value = ExternalAclProvider.convertToExternalPermission(AclPermission.READ);
        Assert.assertEquals("QUERY", value);

        value = ExternalAclProvider.convertToExternalPermission(AclPermission.OPERATION);
        Assert.assertEquals("OPERATION", value);

        value = ExternalAclProvider.convertToExternalPermission(AclPermission.MANAGEMENT);
        Assert.assertEquals("MANAGEMENT", value);

        value = ExternalAclProvider.convertToExternalPermission(AclPermission.ADMINISTRATION);
        Assert.assertEquals("ADMIN", value);

        value = ExternalAclProvider.convertToExternalPermission(AclPermission.DATA_QUERY);
        Assert.assertEquals("DATA_QUERY", value);

        value = ExternalAclProvider.convertToExternalPermission(AclPermission.CREATE);
        Assert.assertEquals(".............................C..", value);
    }

    @Test
    public void testCheckExternalPermission() {
        ExternalAclProvider.checkExternalPermission("ADMIN");
        ExternalAclProvider.checkExternalPermission("MANAGEMENT");
        ExternalAclProvider.checkExternalPermission("OPERATION");
        ExternalAclProvider.checkExternalPermission("QUERY");
    }

    @Test
    public void testCheckExternalPermissionWithEmptyPermission() {
        thrown.expectMessage("Permission should not be empty.");
        ExternalAclProvider.checkExternalPermission("");
    }

    @Test
    public void testCheckExternalPermissionWithInvalidPermission() {
        ExternalAclProvider.checkExternalPermission("ADMIN");
        thrown.expectMessage("Invalid values in parameter \"permission\". "
                + "The value should either be \"ADMIN\", \"MANAGEMENT\", \"OPERATION\" or \"QUERY\".");
        ExternalAclProvider.checkExternalPermission("TEST");
    }
}
