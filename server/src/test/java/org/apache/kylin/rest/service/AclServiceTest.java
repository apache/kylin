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

import java.util.List;

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.springacl.AceImpl;
import org.apache.kylin.rest.security.springacl.AclRecord;
import org.apache.kylin.rest.security.springacl.MutableAclRecord;
import org.apache.kylin.rest.security.springacl.ObjectIdentityImpl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 */
public class AclServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("aclService")
    AclService aclService;

    @Test
    public void testReadFromLegacy() throws Exception {
        ObjectIdentityImpl oid = oid("legacy-test-domain-object");
        MutableAclRecord acl = aclService.readAcl(oid);
        {
            AclRecord rec = acl.getAclRecord();
            Assert.assertEquals(oid, rec.getDomainObjectInfo());
            Assert.assertNull(rec.getParentDomainObjectInfo());
            Assert.assertNull(rec.getParentAcl());
            Assert.assertEquals(new PrincipalSid("ADMIN"), rec.getOwner());
            Assert.assertEquals(true, rec.isEntriesInheriting());

            List<AccessControlEntry> entries = rec.getEntries();
            Assert.assertEquals(2, entries.size());
            AceImpl e0 = (AceImpl) entries.get(0);
            Assert.assertEquals(rec, e0.getAcl());
            Assert.assertEquals(Integer.valueOf(0), e0.getId());
            Assert.assertEquals(new PrincipalSid("ADMIN"), e0.getSid());
            Assert.assertEquals(16, e0.getPermissionMask());
            AceImpl e1 = (AceImpl) entries.get(1);
            Assert.assertEquals(rec, e1.getAcl());
            Assert.assertEquals(Integer.valueOf(1), e1.getId());
            Assert.assertEquals(new PrincipalSid("MODELER"), e1.getSid());
            Assert.assertEquals(1, e1.getPermissionMask());
        }

        aclService.upsertAce(acl, new GrantedAuthoritySid("G1"), AclPermission.MANAGEMENT);
        MutableAclRecord newAcl = aclService.readAcl(oid);
        {
            AclRecord rec = newAcl.getAclRecord();
            List<AccessControlEntry> entries = rec.getEntries();
            Assert.assertEquals(3, entries.size());
            AceImpl e0 = (AceImpl) entries.get(0);
            Assert.assertEquals(rec, e0.getAcl());
            Assert.assertEquals(Integer.valueOf(0), e0.getId());
            Assert.assertEquals(new GrantedAuthoritySid("G1"), e0.getSid());
            Assert.assertEquals(32, e0.getPermissionMask());
        }
    }

    @Test
    public void testBasics() throws Exception {
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
        Assert.assertEquals(null, childAclOutdated.getAclRecord().getParentDomainObjectInfo());
        
        // update permission on an outdated ACL, retry should keep things going
        PrincipalSid user1 = new PrincipalSid("user1");
        MutableAclRecord childAcl2 = aclService.upsertAce(childAclOutdated, user1, AclPermission.ADMINISTRATION);
        Assert.assertEquals(parentOid, childAcl2.getAclRecord().getParentDomainObjectInfo());
        Assert.assertEquals(AclPermission.ADMINISTRATION, childAcl2.getAclRecord().getPermission(user1));
        
        // remove permission
        MutableAclRecord childAcl3 = aclService.upsertAce(childAcl2, user1, null);
        Assert.assertEquals(0, childAcl3.getAclRecord().getEntries().size());
        
        // delete ACL
        aclService.deleteAcl(parentOid, true);
        
        try {
            aclService.readAcl(childOid);
            Assert.fail();
        } catch (NotFoundException ex) {
            // expected
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
