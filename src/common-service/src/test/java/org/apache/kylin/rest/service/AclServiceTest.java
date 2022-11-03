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
package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
public class AclServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    AclService aclService = Mockito.spy(AclService.class);

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(SpringContext.class);

        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);

        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        ApplicationContext applicationContext = PowerMockito.mock(ApplicationContext.class);
        PowerMockito.when(SpringContext.getApplicationContext()).thenReturn(applicationContext);
        PowerMockito.when(SpringContext.getBean(PermissionFactory.class))
                .thenReturn(PowerMockito.mock(PermissionFactory.class));
        PowerMockito.when(SpringContext.getBean(PermissionGrantingStrategy.class))
                .thenReturn(PowerMockito.mock(PermissionGrantingStrategy.class));
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        switchToAdmin();
        ObjectIdentityImpl parentOid = oid("parent-obj");
        MutableAclRecord parentAcl = (MutableAclRecord) aclService.createAcl(parentOid);

        switchToAnalyst();
        ObjectIdentityImpl childOid = oid("child-obj");
        MutableAclRecord childAcl = (MutableAclRecord) aclService.createAcl(childOid);
        MutableAclRecord childAclOutdated = aclService.readAcl(childOid);

        // test create on existing acl
        try {
            aclService.createAcl(childOid);
            Assert.fail();
        } catch (AlreadyExistsException ex) {
            // expected
        }

        // inherit parent
        childAcl = aclService.inherit(childAcl, parentAcl);
        Assert.assertEquals(parentOid, childAcl.getAclRecord().getParentDomainObjectInfo());
        //        Assert.assertEquals(parentOid, childAclOutdated.getAclRecord().getParentDomainObjectInfo());

        // update permission on an outdated ACL, retry should keep things going
        PrincipalSid user1 = new PrincipalSid("user1");
        MutableAclRecord childAcl2 = aclService.upsertAce(childAcl, user1, AclPermission.ADMINISTRATION);
        Assert.assertEquals(parentOid, childAcl2.getAclRecord().getParentDomainObjectInfo());
        Assert.assertEquals(AclPermission.ADMINISTRATION, childAcl2.getAclRecord().getPermission(user1));

        // remove permission
        MutableAclRecord childAcl3 = aclService.upsertAce(childAcl2, user1, null);
        Assert.assertEquals(0, childAcl3.getAclRecord().getEntries().size());

        // delete ACL
        aclService.deleteAcl(parentOid, true);

        Assert.assertNull(aclService.readAcl(childOid));
    }

    @Test
    public void testBatchUpsertAce() {
        switchToAdmin();
        ObjectIdentity oid = oid("acl");
        MutableAclRecord acl = (MutableAclRecord) aclService.createAcl(oid);
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            sidToPerm.put(new PrincipalSid("u" + i), AclPermission.ADMINISTRATION);
        }
        aclService.batchUpsertAce(acl, sidToPerm);

        for (Acl a : aclService.readAclsById(Collections.singletonList(oid)).values()) {
            List<AccessControlEntry> e = a.getEntries();
            Assert.assertEquals(10, e.size());
            for (int i = 0; i < e.size(); i++) {
                Assert.assertEquals(new PrincipalSid("u" + i), e.get(i).getSid());
            }
        }
    }

    private void switchToAdmin() {
        Authentication adminAuth = new TestingAuthenticationToken("ADMIN", "ADMIN", "ROLE_ADMIN");
        SecurityContextHolder.getContext().setAuthentication(adminAuth);
    }

    private void switchToAnalyst() {
        Authentication analystAuth = new TestingAuthenticationToken("ANALYST", "ANALYST", "ROLE_ANALYST");
        SecurityContextHolder.getContext().setAuthentication(analystAuth);
    }

    private ObjectIdentityImpl oid(String oid) {
        return new ObjectIdentityImpl(new MockAclEntity(oid));
    }

    public static class MockAclEntity implements AclEntity {

        private String id;

        /**
         * @param id
         */
        public MockAclEntity(String id) {
            super();
            this.id = id;
        }

        @Override
        public String getId() {
            return id;
        }
    }
}
