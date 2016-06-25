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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.security.AclEntityFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

/**
 * @author xduo
 * 
 */
@Component("accessService")
public class AccessService {

    @Autowired
    private AclService aclService;

    @Autowired
    UserService userService;

    // ~ Methods to manage acl life circle of domain objects ~

    @Transactional
    public Acl init(AclEntity ae, Permission initPermission) {
        Acl acl = null;
        ObjectIdentity objectIdentity = new ObjectIdentityImpl(ae.getClass(), ae.getId());

        try {
            // Create acl record for secured domain object.
            acl = aclService.createAcl(objectIdentity);
        } catch (AlreadyExistsException e) {
            acl = (MutableAcl) aclService.readAclById(objectIdentity);
        }

        if (null != initPermission) {
            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            PrincipalSid sid = new PrincipalSid(auth);
            acl = grant(ae, initPermission, sid);
        }

        return acl;
    }

    @Transactional
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public Acl grant(AclEntity ae, Permission permission, Sid sid) {
        Assert.notNull(ae, "Acl domain object required");
        Assert.notNull(permission, "Acl permission required");
        Assert.notNull(sid, "Sid required");

        ObjectIdentity objectIdentity = new ObjectIdentityImpl(ae.getClass(), ae.getId());
        MutableAcl acl = null;

        try {
            acl = (MutableAcl) aclService.readAclById(objectIdentity);
        } catch (NotFoundException e) {
            acl = (MutableAcl) init(ae, null);
        }

        int indexOfAce = -1;
        for (int i = 0; i < acl.getEntries().size(); i++) {
            AccessControlEntry ace = acl.getEntries().get(i);

            if (ace.getSid().equals(sid)) {
                indexOfAce = i;
            }
        }

        if (indexOfAce != -1) {
            secureOwner(acl, indexOfAce);
            acl.updateAce(indexOfAce, permission);
        } else {
            acl.insertAce(acl.getEntries().size(), permission, sid, true);
        }

        acl = aclService.updateAcl(acl);

        return acl;
    }

    @Transactional
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public Acl update(AclEntity ae, Long accessEntryId, Permission newPermission) {
        Assert.notNull(ae, "Acl domain object required");
        Assert.notNull(accessEntryId, "Ace id required");
        Assert.notNull(newPermission, "Acl permission required");

        ObjectIdentity objectIdentity = new ObjectIdentityImpl(ae.getClass(), ae.getId());
        MutableAcl acl = (MutableAcl) aclService.readAclById(objectIdentity);

        int indexOfAce = -1;
        for (int i = 0; i < acl.getEntries().size(); i++) {
            AccessControlEntry ace = acl.getEntries().get(i);
            if (ace.getId().equals(accessEntryId)) {
                indexOfAce = i;
                break;
            }
        }

        if (indexOfAce != -1) {
            secureOwner(acl, indexOfAce);

            try {
                acl.updateAce(indexOfAce, newPermission);
                acl = aclService.updateAcl(acl);
            } catch (NotFoundException e) {
                //do nothing?
            }
        }

        return acl;
    }

    @Transactional
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public Acl revoke(AclEntity ae, Long accessEntryId) {
        Assert.notNull(ae, "Acl domain object required");
        Assert.notNull(accessEntryId, "Ace id required");

        ObjectIdentity objectIdentity = new ObjectIdentityImpl(ae.getClass(), ae.getId());
        MutableAcl acl = (MutableAcl) aclService.readAclById(objectIdentity);
        int indexOfAce = -1;

        for (int i = 0; i < acl.getEntries().size(); i++) {
            AccessControlEntry ace = acl.getEntries().get(i);
            if (((Long) ace.getId()).equals(accessEntryId)) {
                indexOfAce = i;
                break;
            }
        }

        if (indexOfAce != -1) {
            secureOwner(acl, indexOfAce);

            try {
                acl.deleteAce(indexOfAce);
                acl = aclService.updateAcl(acl);
            } catch (NotFoundException e) {
                //do nothing?
            }
        }

        return acl;
    }

    @Transactional
    public void inherit(AclEntity ae, AclEntity parentAe) {
        Assert.notNull(ae, "Acl domain object required");
        Assert.notNull(parentAe, "Parent acl required");

        ObjectIdentity objectIdentity = new ObjectIdentityImpl(ae.getClass(), ae.getId());
        MutableAcl acl = null;
        try {
            acl = (MutableAcl) aclService.readAclById(objectIdentity);
        } catch (NotFoundException e) {
            acl = (MutableAcl) init(ae, null);
        }

        ObjectIdentity parentObjectIdentity = new ObjectIdentityImpl(parentAe.getClass(), parentAe.getId());
        MutableAcl parentAcl = null;
        try {
            parentAcl = (MutableAcl) aclService.readAclById(parentObjectIdentity);
        } catch (NotFoundException e) {
            parentAcl = (MutableAcl) init(parentAe, null);
        }

        if (null == acl || null == parentAcl) {
            return;
        }

        acl.setEntriesInheriting(true);
        acl.setParent(parentAcl);
        aclService.updateAcl(acl);
    }

    @Transactional
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void clean(AclEntity ae, boolean deleteChildren) {
        Assert.notNull(ae, "Acl domain object required");

        // For those may have null uuid, like DataModel, won't delete Acl.
        if (ae.getId() == null)
            return;

        ObjectIdentity objectIdentity = new ObjectIdentityImpl(ae.getClass(), ae.getId());

        try {
            aclService.deleteAcl(objectIdentity, deleteChildren);
        } catch (NotFoundException e) {
            //do nothing?
        }
    }

    // ~ Methods to get acl info of domain objects ~

    public RootPersistentEntity getAclEntity(String entityType, String uuid) {
        if (null == uuid) {
            return null;
        }

        return AclEntityFactory.createAclEntity(entityType, uuid);
    }

    public Acl getAcl(AclEntity ae) {
        if (null == ae) {
            return null;
        }
        ObjectIdentity objectIdentity = new ObjectIdentityImpl(ae.getClass(), ae.getId());
        Acl acl = null;

        try {
            acl = (MutableAcl) aclService.readAclById(objectIdentity);
        } catch (NotFoundException e) {
            //do nothing?
        }

        return acl;
    }

    public Sid getSid(String sid, boolean isPrincepal) {
        if (isPrincepal) {
            return new PrincipalSid(sid);
        } else {
            return new GrantedAuthoritySid(sid);
        }
    }

    public List<AccessEntryResponse> generateAceResponses(Acl acl) {
        if (null == acl) {
            return Collections.emptyList();
        }
        List<AccessEntryResponse> accessControlEntities = new ArrayList<AccessEntryResponse>();

        // Cause there is a circle reference in AccessControlEntry, it needs to
        // set acl to null as a workaround.
        for (AccessControlEntry ace : acl.getEntries()) {
            accessControlEntities.add(new AccessEntryResponse(ace.getId(), ace.getSid(), ace.getPermission(), ace.isGranting()));
        }

        return accessControlEntities;
    }

    /**
     * Protect admin permission granted to acl owner.
     * 
     * @param acl
     * @param indexOfAce
     */
    private void secureOwner(MutableAcl acl, int indexOfAce) {
        // Can't revoke admin permission from domain object owner
        if (acl.getOwner().equals(acl.getEntries().get(indexOfAce).getSid()) && BasePermission.ADMINISTRATION.equals(acl.getEntries().get(indexOfAce).getPermission())) {
            throw new ForbiddenException("Can't revoke admin permission of owner.");
        }
    }
}
