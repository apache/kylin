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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.acls.domain.ConsoleAuditLogger;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class KylinPermissionGrantingStrategyTest {

    @Test
    public void testIsGranted() {
        val permissionGrantingStrategy = new KylinPermissionGrantingStrategy(new ConsoleAuditLogger());
        PrincipalSid sid = new PrincipalSid("admin");
        AceImpl ace = new AceImpl(sid, AclPermission.MANAGEMENT);
        fillAceImp(ace);
        Assert.assertTrue(permissionGrantingStrategy.isGranted(ace, AclPermission.MANAGEMENT));
        Assert.assertFalse(permissionGrantingStrategy.isGranted(ace, AclPermission.DATA_QUERY));

        ace.setPermission(
                new CompositeAclPermission(AclPermission.MANAGEMENT, Arrays.asList(AclPermission.DATA_QUERY)));
        Assert.assertTrue(ace.getPermission() instanceof CompositeAclPermission);
        Assert.assertTrue(permissionGrantingStrategy.isGranted(ace, AclPermission.MANAGEMENT));
        Assert.assertTrue(permissionGrantingStrategy.isGranted(ace, AclPermission.DATA_QUERY));
        Assert.assertFalse(permissionGrantingStrategy.isGranted(ace, new AclPermission(0)));

        Assert.assertEquals(new AceImpl(sid, null), (new AceImpl(sid, new AclPermission(0))));
        Assert.assertNotEquals(new AceImpl(sid, AclPermission.MANAGEMENT), (new AceImpl(sid, new AclPermission(0))));
        Assert.assertNotEquals(new AceImpl(sid, AclPermission.MANAGEMENT).hashCode(), (new AceImpl(sid, new AclPermission(0))).hashCode());

        ace.setPermission(AclPermission.MANAGEMENT);
        Assert.assertEquals(AclPermission.MANAGEMENT, ace.getPermission());
    }

    private void fillAceImp(AceImpl ace) {
        val aclRecord = new AclRecord();
        ReflectionTestUtils.setField(aclRecord, "aclPermissionFactory", new AclPermissionFactory());
        ReflectionTestUtils.setField(ace, "acl", aclRecord);
    }
}
