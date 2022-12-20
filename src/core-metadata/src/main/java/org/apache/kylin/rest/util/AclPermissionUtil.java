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

import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.AclEntityFactory;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.security.CompositeAclPermission;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AclPermissionUtil {

    private AclPermissionUtil() {
    }

    public static List<String> transformAuthorities(Collection<? extends GrantedAuthority> authorities) {
        List<String> ret = Lists.newArrayList();
        for (GrantedAuthority auth : authorities) {
            if (!ret.contains(auth.getAuthority())) {
                ret.add(auth.getAuthority());
            }
        }
        return ret;
    }

    public static String getCurrentUsername() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return Objects.isNull(auth) ? null : auth.getName();
    }

    public static MutableAclRecord getProjectAcl(String project) {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        AclEntity ae = AclEntityFactory.createAclEntity(AclEntityType.PROJECT_INSTANCE, projectInstance.getUuid());
        return AclManager.getInstance(KylinConfig.getInstanceFromEnv()).readAcl(new ObjectIdentityImpl(ae));
    }

    public static Set<String> filterGroupsInProject(Set<String> groups, MutableAclRecord acl) {
        if (Objects.isNull(acl)) {
            return groups;
        }
        Sid sid;
        Set<String> groupsInProject = Sets.newHashSet();
        for (AccessControlEntry ace : acl.getEntries()) {
            sid = ace.getSid();
            if (sid instanceof PrincipalSid) {
                continue;
            }
            groupsInProject.add(getName(sid));
        }
        return groups.stream().filter(groupsInProject::contains).collect(Collectors.toSet());
    }

    public static Set<String> filterGroupsInProject(MutableAclRecord acl) {
        if (Objects.isNull(acl)) {
            return Collections.emptySet();
        }

        return acl.getEntries().parallelStream().filter(ace -> !(ace.getSid() instanceof PrincipalSid))
                .map(ace -> getName(ace.getSid())).collect(Collectors.toSet());
    }

    public static boolean isAdmin(Set<String> groups) {
        return Objects.nonNull(groups) && groups.stream().anyMatch(Constant.ROLE_ADMIN::equals);
    }

    public static boolean isProjectAdminPermission(String permission) {
        return AclPermission.ADMINISTRATION.equals(AclPermissionFactory.getPermission(permission));
    }

    public static boolean isAdmin() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return Objects.nonNull(auth) && auth.getAuthorities().stream().map(GrantedAuthority::getAuthority)
                .anyMatch(Constant.ROLE_ADMIN::equals);
    }

    public static boolean canUseACLGreenChannel(String project, Set<String> groups) {
        return !KylinConfig.getInstanceFromEnv().isAclTCREnabled() || hasProjectAdminPermission(project, groups);
    }

    public static boolean hasProjectAdminPermission(String project, Set<String> groups) {
        return isAdmin() || isAdminInProject(project, groups);
    }

    public static boolean isAdminInProject(String project, Set<String> usergroups) {
        return isSpecificPermissionInProject(project, usergroups, BasePermission.ADMINISTRATION);
    }

    public static boolean isSpecificPermissionInProject(String project, Set<String> userGroups, Permission permission) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (Objects.isNull(auth)) {
            return false;
        }
        MutableAclRecord acl = getProjectAcl(project);
        Set<String> groups = filterGroupsInProject(userGroups, acl);
        return isSpecificPermissionInProject(auth.getName(), groups, permission, acl);
    }

    public static boolean isSpecificPermissionInProject(String username, Set<String> userGroupsInProject,
            Permission aclPermission, MutableAclRecord acl) {
        if (Objects.isNull(acl)) {
            return false;
        }
        Sid sid;
        for (AccessControlEntry ace : acl.getEntries()) {
            if ((ace.getPermission().getMask() & aclPermission.getMask()) != 0) {
                sid = ace.getSid();
                if (isCurrentUser(sid, username)) {
                    return true;
                }
                if (isCurrentGroup(sid, userGroupsInProject)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isSpecificPermissionInProject(String group, String project, Permission aclPermission) {
        MutableAclRecord acl = getProjectAcl(project);
        return isSpecificPermissionInProject(group, aclPermission, acl);
    }

    public static boolean isSpecificPermissionInProject(String group, Permission aclPermission, MutableAclRecord acl) {
        if (Objects.isNull(acl)) {
            return false;
        }
        Sid sid;
        for (AccessControlEntry ace : acl.getEntries()) {
            if ((ace.getPermission().getMask() & aclPermission.getMask()) != 0) {
                sid = ace.getSid();
                if (isCurrentGroup(sid, Sets.newHashSet(group))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isCurrentUser(Sid sid, String username) {
        return (sid instanceof PrincipalSid) && (username.equals(((PrincipalSid) sid).getPrincipal()));
    }

    private static boolean isCurrentGroup(Sid sid, Set<String> groups) {
        if (!(sid instanceof GrantedAuthoritySid)) {
            return false;
        }
        for (String group : groups) {
            if (group.equals(((GrantedAuthoritySid) sid).getGrantedAuthority())) {
                return true;
            }
        }
        return false;
    }

    public static String objID(ObjectIdentity domainObjId) {
        return String.valueOf(domainObjId.getIdentifier());
    }

    public static String getName(Sid sid) {
        if (sid instanceof PrincipalSid) {
            return ((PrincipalSid) sid).getPrincipal();
        } else {
            return ((GrantedAuthoritySid) sid).getGrantedAuthority();
        }
    }

    public static boolean isAclUpdatable(String project, Set<String> groups) {
        return (isAdmin() || (isAdminInProject(project, groups)
                && KylinConfig.getInstanceFromEnv().isAllowedProjectAdminGrantAcl()));
    }

    public static void checkAclUpdatable(String project, Set<String> groups) {
        if (!AclPermissionUtil.isAclUpdatable(project, groups)) {
            checkIfAllowedProjectAdminGrantAcl(KylinConfig.getInstanceFromEnv().isAllowedProjectAdminGrantAcl());
        }
    }

    private static void checkIfAllowedProjectAdminGrantAcl(boolean isAllowedProjectAdminGrantAcl) {
        if (isAllowedProjectAdminGrantAcl) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getAccessDenyOnlyAdminAndProjectAdmin());
        } else {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getAccessDenyOnlyAdmin());
        }
    }

    public static QueryContext.AclInfo createAclInfo(String project, Set<String> groups) {
        return new QueryContext.AclInfo(getCurrentUsername(), groups, isAdminInProject(project, groups));
    }

    public static boolean hasExtPermission(Permission permission) {
        return (permission instanceof CompositeAclPermission
                && CollectionUtils.isNotEmpty(((CompositeAclPermission) permission).getExtMasks()));
    }

    public static Permission modifyBasePermission(Permission sourcePermission, Permission targetPermission) {
        Permission newPermission = targetPermission;
        if (sourcePermission instanceof CompositeAclPermission) {
            List<Permission> extPermissions = ((CompositeAclPermission) sourcePermission).getExtPermissions();
            newPermission = new CompositeAclPermission(targetPermission, extPermissions);
        }
        return newPermission;
    }

    public static Permission addExtPermission(Permission permission, Permission extPermission) {
        CompositeAclPermission compositeAclPermission = convertToCompositePermission(permission);
        compositeAclPermission.addExtPermission(extPermission);
        return compositeAclPermission;
    }

    public static CompositeAclPermission convertToCompositePermission(Permission permission) {
        if (permission instanceof CompositeAclPermission) {
            return (CompositeAclPermission) permission;
        }
        return new CompositeAclPermission(permission);
    }

    public static Permission convertToBasePermission(Permission permission) {
        if (permission instanceof CompositeAclPermission) {
            return ((CompositeAclPermission) permission).getBasePermission();
        }
        return permission;
    }

    public static boolean hasQueryPermission(Permission permission) {
        return (permission.getMask() & AclPermission.DATA_QUERY.getMask()) != 0;
    }
}
