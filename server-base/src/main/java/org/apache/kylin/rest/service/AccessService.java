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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.security.AclEntityFactory;
import org.apache.kylin.rest.security.AclEntityType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;

/**
 * @author xduo
 * 
 */
@Component("accessService")
public class AccessService {

    @Autowired
    @Qualifier("aclService")
    private AclService aclService;

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
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());
        if (permission == null)
            throw new BadRequestException(msg.getACL_PERMISSION_REQUIRED());
        if (sid == null)
            throw new BadRequestException(msg.getSID_REQUIRED());

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
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());
        if (accessEntryId == null)
            throw new BadRequestException(msg.getACE_ID_REQUIRED());
        if (newPermission == null)
            throw new BadRequestException(msg.getACL_PERMISSION_REQUIRED());

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
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());
        if (accessEntryId == null)
            throw new BadRequestException(msg.getACE_ID_REQUIRED());

        MutableAcl acl = (MutableAcl) getAcl(ae);
        int indexOfAce = getIndexOfAce(accessEntryId, acl);
        acl = deleteAndUpdate(acl, indexOfAce);
        return acl;
    }

    private int getIndexOfAce(Long accessEntryId, MutableAcl acl) {
        int indexOfAce = -1;
        List<AccessControlEntry> aces = acl.getEntries();
        for (int i = 0; i < aces.size(); i++) {
            if (aces.get(i).getId().equals(accessEntryId)) {
                indexOfAce = i;
                break;
            }
        }
        return indexOfAce;
    }

    private MutableAcl deleteAndUpdate(MutableAcl acl, int indexOfAce) {
        if (indexOfAce != -1) {
            secureOwner(acl, indexOfAce);
            try {
                acl.deleteAce(indexOfAce);
                acl = aclService.updateAcl(acl);
            } catch (NotFoundException e) {
                throw new RuntimeException("Revoke acl fail." + e.getMessage());
            }
        }
        return acl;
    }

    @Deprecated
    @Transactional
    public void inherit(AclEntity ae, AclEntity parentAe) {
        Message msg = MsgPicker.getMsg();

        if (ae == null) {
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());
        }
        if (parentAe == null) {
            throw new BadRequestException(msg.getPARENT_ACL_NOT_FOUND());
        }

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
        Message msg = MsgPicker.getMsg();

        if (ae == null) {
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());
        }

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

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN +
            " or hasPermission(#ae, 'ADMINISTRATION')" +
            " or hasPermission(#ae, 'MANAGEMENT')" +
            " or hasPermission(#ae, 'OPERATION')" +
            " or hasPermission(#ae, 'READ')")
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

    public List<AccessEntryResponse> generateAceResponsesByFuzzMatching(Acl acl, String nameSeg, boolean isCaseSensitive) {
        if (null == acl) {
            return Collections.emptyList();
        }

        List<AccessEntryResponse> result = new ArrayList<AccessEntryResponse>();
        for (AccessControlEntry ace : acl.getEntries()) {
            if (nameSeg != null && !needAdd(nameSeg, isCaseSensitive, getName(ace.getSid()))) {
                continue;
            }
            result.add(new AccessEntryResponse(ace.getId(), ace.getSid(), ace.getPermission(), ace.isGranting()));
        }

        return result;
    }

    private boolean needAdd(String nameSeg, boolean isCaseSensitive, String name) {
        return isCaseSensitive && StringUtils.contains(name, nameSeg)
                || !isCaseSensitive && StringUtils.containsIgnoreCase(name, nameSeg);
    }

    private static String getName(Sid sid) {
        if (sid instanceof PrincipalSid) {
            return ((PrincipalSid) sid).getPrincipal();
        } else {
            return ((GrantedAuthoritySid) sid).getGrantedAuthority();
        }
    }

    public List<AccessEntryResponse> generateAceResponses(Acl acl) {
        return generateAceResponsesByFuzzMatching(acl, null, false);
    }

    public List<String> getAllAclSids(Acl acl, String type) {
        if (null == acl) {
            return Collections.emptyList();
        }

        List<String> result = new ArrayList<>();
        for (AccessControlEntry ace : acl.getEntries()) {
            String name = null;
            if (type.equalsIgnoreCase("user") && ace.getSid() instanceof PrincipalSid) {
                name = ((PrincipalSid) ace.getSid()).getPrincipal();
            }
            if (type.equalsIgnoreCase("group") && ace.getSid() instanceof GrantedAuthoritySid) {
                name = ((GrantedAuthoritySid) ace.getSid()).getGrantedAuthority();
            }
            if (!StringUtils.isBlank(name)) {
                result.add(name);
            }
        }
        return result;
    }

    /**
     * Protect admin permission granted to acl owner.
     *
     * @param acl
     * @param indexOfAce
     */
    private void secureOwner(MutableAcl acl, int indexOfAce) {
        Message msg = MsgPicker.getMsg();

        // Can't revoke admin permission from domain object owner
        if (acl.getOwner().equals(acl.getEntries().get(indexOfAce).getSid())
                && BasePermission.ADMINISTRATION.equals(acl.getEntries().get(indexOfAce).getPermission())) {
            throw new ForbiddenException(msg.getREVOKE_ADMIN_PERMISSION());
        }
    }

    public Object generateAllAceResponses(Acl acl) {
        List<AccessEntryResponse> result = new ArrayList<AccessEntryResponse>();

        while (acl != null) {
            for (AccessControlEntry ace : acl.getEntries()) {
                result.add(new AccessEntryResponse(ace.getId(), ace.getSid(), ace.getPermission(), ace.isGranting()));
            }
            acl = acl.getParentAcl();
        }

        return result;
    }

    public void revokeProjectPermission(String name, String type) {
        //revoke user's project permission
        List<ProjectInstance> projectInstances = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects();
        for (ProjectInstance pi : projectInstances) {
            // after KYLIN-2760, only project ACL will work, so entity type is always ProjectInstance.
            AclEntity ae = getAclEntity("ProjectInstance", pi.getUuid());

            MutableAcl acl = (MutableAcl) getAcl(ae);
            if (acl == null) {
                return;
            }
            List<AccessControlEntry> aces = acl.getEntries();
            if (aces == null) {
                return;
            }

            int indexOfAce = -1;
            for (int i = 0; i < aces.size(); i++) {
                if (needRevoke(aces.get(i).getSid(), name, type)) {
                    indexOfAce = i;
                    break;
                }
            }
            deleteAndUpdate(acl, indexOfAce);
        }
    }

    public String getUserPermissionInPrj(String project) {
        String grantedPermission = "";
        List<String> groups = getGroupsFromCurrentUser();
        if (groups.contains(Constant.ROLE_ADMIN)) {
            return "GLOBAL_ADMIN";
        }

        // {user/group:permission}
        Map<String, Integer> projectPermissions = getProjectPermission(project);
        Integer greaterPermission = projectPermissions.get(SecurityContextHolder.getContext().getAuthentication().getName());
        for (String group : groups) {
            Integer groupPerm = projectPermissions.get(group);
            greaterPermission = Preconditions.checkNotNull(getGreaterPerm(groupPerm, greaterPermission));
        }

        switch (greaterPermission) {
            case 16:
                grantedPermission = "ADMINISTRATION";
                break;
            case 32:
                grantedPermission = "MANAGEMENT";
                break;
            case 64:
                grantedPermission = "OPERATION";
                break;
            case 1:
                grantedPermission = "READ";
                break;
            case 0:
                grantedPermission = "EMPTY";
                break;
            default:
                throw new RuntimeException("invalid permission state:" + greaterPermission);
        }
        return grantedPermission;
    }

    private Map<String, Integer> getProjectPermission(String project) {
        Map<String, Integer> SidWithPermission = new HashMap<>();

        String uuid = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).getUuid();
        AclEntity ae = getAclEntity(AclEntityType.PROJECT_INSTANCE, uuid);
        Acl acl = getAcl(ae);
        if (acl != null && acl.getEntries() != null) {
            List<AccessControlEntry> aces = acl.getEntries();
            for (AccessControlEntry ace : aces) {
                Sid sid = ace.getSid();
                if (sid instanceof PrincipalSid) {
                    String principal = ((PrincipalSid) sid).getPrincipal();
                    SidWithPermission.put(principal, ace.getPermission().getMask());
                }
                if (sid instanceof GrantedAuthoritySid) {
                    String grantedAuthority = ((GrantedAuthoritySid) sid).getGrantedAuthority();
                    SidWithPermission.put(grantedAuthority, ace.getPermission().getMask());
                }
            }
        }
        return SidWithPermission;
    }

    private List<String> getGroupsFromCurrentUser() {
        List<String> groups = new ArrayList<>();
        Collection<? extends GrantedAuthority> authorities = SecurityContextHolder.getContext().getAuthentication().getAuthorities();

        for (GrantedAuthority auth : authorities) {
            groups.add(auth.getAuthority());
        }
        return groups;
    }

    private Integer getGreaterPerm(Integer mask1, Integer mask2) {
        if (mask1 == null && mask2 == null) {
            return 0;
        }
        if (mask1 != null && mask2 == null) {
            return mask1;
        }

        if (mask1 == null && mask2 != null) {
            return mask2;
        }

        if (mask1 == 16 || mask2 == 16) { //ADMIN
            return 16;
        }
        if (mask1 == 32 || mask2 == 32) { //MANAGEMENT
            return 32;
        }
        if (mask1 == 64 || mask2 == 64) { //OPERATOR
            return 64;
        }
        if (mask1 == 1 || mask2 == 1) { // READ
            return 1;
        }
        return null;
    }

    private boolean needRevoke(Sid sid, String name, String type) {
        if (type.equals(MetadataConstants.TYPE_USER) && sid instanceof PrincipalSid) {
            return ((PrincipalSid) sid).getPrincipal().equals(name);
        } else if (type.equals(MetadataConstants.TYPE_GROUP) && sid instanceof GrantedAuthoritySid) {
            return ((GrantedAuthoritySid) sid).getGrantedAuthority().equals(name);
        } else {
            return false;
        }
    }
}
