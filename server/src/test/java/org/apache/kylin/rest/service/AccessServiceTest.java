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

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Sid;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author xduo
 */
public class AccessServiceTest extends ServiceTestBase {

    @Autowired
    AccessService accessService;

    @Test
    public void testBasics() throws JsonProcessingException {
        Sid adminSid = accessService.getSid("ADMIN", true);
        Assert.assertNotNull(adminSid);
        Assert.assertNotNull(AclPermissionFactory.getPermissions());

        AclEntity ae = new MockAclEntity("test-domain-object");
        accessService.clean(ae, true);
        AclEntity attachedEntity = new MockAclEntity("attached-domain-object");
        accessService.clean(attachedEntity, true);

        // test getAcl
        Acl acl = accessService.getAcl(ae);
        Assert.assertNull(acl);

        // test init
        acl = accessService.init(ae, AclPermission.ADMINISTRATION);
        Assert.assertTrue(((PrincipalSid) acl.getOwner()).getPrincipal().equals("ADMIN"));
        Assert.assertEquals(accessService.generateAceResponses(acl).size(), 1);
        AccessEntryResponse aer = accessService.generateAceResponses(acl).get(0);
        Assert.assertTrue(aer.getId() != null);
        Assert.assertTrue(aer.getPermission() == AclPermission.ADMINISTRATION);
        Assert.assertTrue(((PrincipalSid) aer.getSid()).getPrincipal().equals("ADMIN"));

        // test grant
        Sid modeler = accessService.getSid("MODELER", true);
        acl = accessService.grant(ae, AclPermission.ADMINISTRATION, modeler);
        Assert.assertEquals(accessService.generateAceResponses(acl).size(), 2);

        Long modelerEntryId = null;
        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Long) ace.getId();
                Assert.assertTrue(ace.getPermission() == AclPermission.ADMINISTRATION);
            }
        }

        // test update
        acl = accessService.update(ae, modelerEntryId, AclPermission.READ);

        Assert.assertEquals(accessService.generateAceResponses(acl).size(), 2);

        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Long) ace.getId();
                Assert.assertTrue(ace.getPermission() == AclPermission.READ);
            }
        }

        accessService.clean(attachedEntity, true);

        Acl attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertNull(attachedEntityAcl);
        attachedEntityAcl = accessService.init(attachedEntity, AclPermission.ADMINISTRATION);

        accessService.inherit(attachedEntity, ae);

        attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertTrue(attachedEntityAcl.getParentAcl() != null);
        Assert.assertTrue(attachedEntityAcl.getParentAcl().getObjectIdentity().getIdentifier().equals("test-domain-object"));
        Assert.assertTrue(attachedEntityAcl.getEntries().size() == 1);

        // test revoke
        acl = accessService.revoke(ae, modelerEntryId);
        Assert.assertEquals(accessService.generateAceResponses(acl).size(), 1);

        // test clean
        accessService.clean(ae, true);
        acl = accessService.getAcl(ae);
        Assert.assertNull(acl);

        attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertNull(attachedEntityAcl);
    }

    public class MockAclEntity implements AclEntity {

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
