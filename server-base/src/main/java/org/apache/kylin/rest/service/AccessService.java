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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
import org.apache.kylin.rest.security.springacl.AclRecord;
import org.apache.kylin.rest.security.springacl.MutableAclRecord;
import org.apache.kylin.rest.security.springacl.ObjectIdentityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

@Component("accessService")
public class AccessService {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(AccessService.class);

    @Autowired
    @Qualifier("aclService")
    private AclService aclService;

    // ~ Methods to manage acl life circle of domain objects ~

    @Transactional
    public MutableAclRecord init(AclEntity ae, Permission initPermission) {
        MutableAclRecord acl = null;
        ObjectIdentity objectIdentity = new ObjectIdentityImpl(ae);

        try {
            // Create acl record for secured domain object.
            acl = (MutableAclRecord) aclService.createAcl(objectIdentity);
        } catch (AlreadyExistsException e) {
            acl = aclService.readAcl(objectIdentity);
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
    public void batchGrant(AclEntity ae, Map<Sid, Permission> sidToPerm) {
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());
        if (sidToPerm == null)
            throw new BadRequestException(msg.getACL_PERMISSION_REQUIRED());

        MutableAclRecord acl;
        try {
            acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        } catch (NotFoundException e) {
            acl = init(ae, null);
        }

        for (Sid sid : sidToPerm.keySet()) {
            secureOwner(acl, sid);
        }
        aclService.batchUpsertAce(acl, sidToPerm);
    }

    @Transactional
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public MutableAclRecord grant(AclEntity ae, Permission permission, Sid sid) {
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());
        if (permission == null)
            throw new BadRequestException(msg.getACL_PERMISSION_REQUIRED());
        if (sid == null)
            throw new BadRequestException(msg.getSID_REQUIRED());

        MutableAclRecord acl = null;
        try {
            acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        } catch (NotFoundException e) {
            acl = init(ae, null);
        }

        secureOwner(acl, sid);

        return aclService.upsertAce(acl, sid, permission);
    }

    @Transactional
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public MutableAclRecord update(AclEntity ae, int accessEntryIndex, Permission newPermission) {
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());
        if (newPermission == null)
            throw new BadRequestException(msg.getACL_PERMISSION_REQUIRED());

        MutableAclRecord acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        Sid sid = acl.getAclRecord().getAccessControlEntryAt(accessEntryIndex).getSid();

        secureOwner(acl, sid);

        return aclService.upsertAce(acl, sid, newPermission);
    }

    @Transactional
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public MutableAclRecord revoke(AclEntity ae, int accessEntryIndex) {
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());

        MutableAclRecord acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        Sid sid = acl.getAclRecord().getAccessControlEntryAt(accessEntryIndex).getSid();

        secureOwner(acl, sid);

        return aclService.upsertAce(acl, sid, null);
    }

    /**
     * The method is not used at the moment
     */
    @Transactional
    public void inherit(AclEntity ae, AclEntity parentAe) {
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new BadRequestException(msg.getACL_DOMAIN_NOT_FOUND());
        if (parentAe == null)
            throw new BadRequestException(msg.getPARENT_ACL_NOT_FOUND());

        MutableAclRecord acl = null;
        try {
            acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        } catch (NotFoundException e) {
            acl = init(ae, null);
        }

        MutableAclRecord parentAcl = null;
        try {
            parentAcl = aclService.readAcl(new ObjectIdentityImpl(parentAe));
        } catch (NotFoundException e) {
            parentAcl = init(parentAe, null);
        }

        if (null == acl || null == parentAcl) {
            return;
        }

        aclService.inherit(acl, parentAcl);
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

        ObjectIdentity objectIdentity = new ObjectIdentityImpl(ae);

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

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')"
            + " or hasPermission(#ae, 'MANAGEMENT')" + " or hasPermission(#ae, 'OPERATION')"
            + " or hasPermission(#ae, 'READ')")
    public MutableAclRecord getAcl(AclEntity ae) {
        if (null == ae) {
            return null;
        }

        MutableAclRecord acl = null;
        try {
            acl = aclService.readAcl(new ObjectIdentityImpl(ae));
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

    public List<AccessEntryResponse> generateAceResponsesByFuzzMatching(Acl acl, String nameSeg,
            boolean isCaseSensitive) {
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
            if (type.equalsIgnoreCase(MetadataConstants.TYPE_USER) && ace.getSid() instanceof PrincipalSid) {
                name = ((PrincipalSid) ace.getSid()).getPrincipal();
            }
            if (type.equalsIgnoreCase(MetadataConstants.TYPE_GROUP) && ace.getSid() instanceof GrantedAuthoritySid) {
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
     */
    private void secureOwner(MutableAclRecord acl, Sid sid) {
        Message msg = MsgPicker.getMsg();

        AclRecord record = acl.getAclRecord();
        if (record.getOwner().equals(sid) == false)
            return;

        // prevent changing owner's admin permission
        if (BasePermission.ADMINISTRATION.equals(record.getPermission(sid)))
            throw new ForbiddenException(msg.getREVOKE_ADMIN_PERMISSION());
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
        Sid sid = null;
        if (type.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            sid = new PrincipalSid(name);
        } else if (type.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            sid = new GrantedAuthoritySid(name);
        } else {
            return;
        }

        // revoke user's project permission
        List<ProjectInstance> projectInstances = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects();
        for (ProjectInstance pi : projectInstances) {
            // after KYLIN-2760, only project ACL will work, so entity type is always ProjectInstance.
            AclEntity ae = getAclEntity("ProjectInstance", pi.getUuid());

            MutableAclRecord acl = getAcl(ae);
            if (acl == null) {
                return;
            }

            Permission perm = acl.getAclRecord().getPermission(sid);
            if (perm != null) {
                secureOwner(acl, sid);
                aclService.upsertAce(acl, sid, null);
            }
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
        Integer greaterPermission = projectPermissions
                .get(SecurityContextHolder.getContext().getAuthentication().getName());
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
        Map<String, Integer> SidWithPermission = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

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
        Collection<? extends GrantedAuthority> authorities = SecurityContextHolder.getContext().getAuthentication()
                .getAuthorities();

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
}
