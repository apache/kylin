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

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_USERGROUP_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.USER_NOT_EXIST;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;
import static org.springframework.security.acls.domain.BasePermission.ADMINISTRATION;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.SidPermissionWithAclResponse;
import org.apache.kylin.rest.security.AclEntityFactory;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.security.AclRecord;
import org.apache.kylin.rest.security.ExternalAclProvider;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.common.persistence.transaction.AccessBatchGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.AccessGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.AccessRevokeEventNotifier;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.response.AclTCRResponse;
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
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;

@Component("accessService")
public class AccessService extends BasicService {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(AccessService.class);

    @Autowired
    @Qualifier("aclService")
    private AclService aclService;

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @Autowired(required = false)
    @Qualifier("aclTCRService")
    private AclTCRServiceSupporter aclTCRService;

    @Transaction
    public MutableAclRecord init(AclEntity ae, Permission initPermission) {
        MutableAclRecord acl;
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

    @Transaction
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void batchGrant(List<AccessRequest> requests, AclEntity ae) {
        Map<Sid, Permission> sid2perm = requests.stream().map(r -> {
            Sid sid = getSid(r.getSid(), r.isPrincipal());
            Permission permission = AclPermissionFactory.getPermission(r.getPermission());
            if (Objects.nonNull(sid) && Objects.nonNull(permission)) {
                return new AbstractMap.SimpleEntry<>(sid, permission);
            } else {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        batchGrant(ae, sid2perm);
    }

    @Transaction
    void batchGrant(AclEntity ae, Map<Sid, Permission> sidToPerm) {
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new KylinException(INVALID_PARAMETER, msg.getAclDomainNotFound());
        if (sidToPerm == null)
            throw new KylinException(PERMISSION_DENIED, msg.getAclPermissionRequired());

        MutableAclRecord acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        if (Objects.isNull(acl)) {
            acl = init(ae, null);
        }

        for (Sid sid : sidToPerm.keySet()) {
            secureOwner(acl, sid);
        }
        aclService.batchUpsertAce(acl, sidToPerm);
    }

    @Transaction
    MutableAclRecord grant(AclEntity ae, Permission permission, Sid sid) {
        Message msg = MsgPicker.getMsg();
        if (ae == null)
            throw new KylinException(INVALID_PARAMETER, msg.getAclDomainNotFound());
        if (permission == null)
            throw new KylinException(PERMISSION_DENIED, msg.getAclPermissionRequired());
        if (sid == null)
            throw new KylinException(EMPTY_USER_NAME, msg.getSidRequired());

        MutableAclRecord acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        if (Objects.isNull(acl)) {
            acl = init(ae, null);
        }

        secureOwner(acl, sid);

        return aclService.upsertAce(acl, sid, permission);
    }

    @Transaction
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void grant(AclEntity ae, String identifier, Boolean isPrincipal, String permission) {
        Sid sid = getSid(identifier, isPrincipal);
        grant(ae, AclPermissionFactory.getPermission(permission), sid);
    }

    @Transaction
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public MutableAclRecord update(AclEntity ae, int accessEntryIndex, Permission newPermission) {
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new KylinException(INVALID_PARAMETER, msg.getAclDomainNotFound());
        if (newPermission == null)
            throw new KylinException(PERMISSION_DENIED, msg.getAclPermissionRequired());

        MutableAclRecord acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        Sid sid = acl.getAclRecord().getAccessControlEntryAt(accessEntryIndex).getSid();

        secureOwner(acl, sid);

        return aclService.upsertAce(acl, sid, newPermission);
    }

    @Transaction
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public MutableAclRecord revoke(AclEntity ae, int accessEntryIndex) {
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new KylinException(INVALID_PARAMETER, msg.getAclDomainNotFound());

        MutableAclRecord acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        Sid sid = acl.getAclRecord().getAccessControlEntryAt(accessEntryIndex).getSid();

        secureOwner(acl, sid);

        return aclService.upsertAce(acl, sid, null);
    }

    @Transaction
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public MutableAclRecord revokeWithSid(AclEntity ae, String name, boolean principal) {
        Message msg = MsgPicker.getMsg();
        if (Objects.isNull(ae)) {
            throw new KylinException(INVALID_PARAMETER, msg.getAclDomainNotFound());
        }
        MutableAclRecord acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        Sid sid = acl.getAclRecord().getAceBySidAndPrincipal(name, principal);

        secureOwner(acl, sid);
        return aclService.upsertAce(acl, sid, null);
    }

    @Transaction
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void batchRevoke(AclEntity ae, List<AccessRequest> requests) {
        Message msg = MsgPicker.getMsg();
        if (ae == null)
            throw new KylinException(INVALID_PARAMETER, msg.getAclDomainNotFound());

        Permission emptyPermission = BasePermission.READ;
        Map<Sid, Permission> sid2perm = requests.stream()
                .map(r -> new AbstractMap.SimpleEntry<>(getSid(r.getSid(), r.isPrincipal()), emptyPermission))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        sid2perm.entrySet().forEach(e -> e.setValue(null));
        batchGrant(ae, sid2perm);
    }

    /**
     * The method is not used at the moment
     */
    void inherit(AclEntity ae, AclEntity parentAe) {
        Message msg = MsgPicker.getMsg();

        if (ae == null)
            throw new KylinException(INVALID_PARAMETER, msg.getAclDomainNotFound());
        if (parentAe == null)
            throw new KylinException(INVALID_PARAMETER, msg.getParentAclNotFound());

        MutableAclRecord acl = aclService.readAcl(new ObjectIdentityImpl(ae));
        if (Objects.isNull(acl)) {
            acl = init(ae, null);
        }

        MutableAclRecord parentAcl = aclService.readAcl(new ObjectIdentityImpl(parentAe));
        if (Objects.isNull(parentAcl)) {
            parentAcl = init(parentAe, null);
        }

        if (null == acl || null == parentAcl) {
            return;
        }

        aclService.inherit(acl, parentAcl);
    }

    @Transaction
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
        List<ProjectInstance> projectInstances = getManager(NProjectManager.class).listAllProjects();
        for (ProjectInstance pi : projectInstances) {
            // after KYLIN-2760, only project ACL will work, so entity type is always ProjectInstance.
            AclEntity ae = getAclEntity("ProjectInstance", pi.getUuid());

            MutableAclRecord acl = getAcl(ae);
            if (Objects.isNull(acl)) {
                // skip empty project
                continue;
            }

            Permission perm = acl.getAclRecord().getPermission(sid);
            if (perm != null) {
                secureOwner(acl, sid);
                aclService.upsertAce(acl, sid, null);
            }
        }
    }

    @Transaction
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void clean(AclEntity ae, boolean deleteChildren) {
        Message msg = MsgPicker.getMsg();

        if (ae == null) {
            throw new KylinException(INVALID_PARAMETER, msg.getAclDomainNotFound());
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
        return aclService.readAcl(new ObjectIdentityImpl(ae));
    }

    public Sid getSid(String sid, boolean isPrincipal) {
        if (isPrincipal) {
            return new PrincipalSid(sid);
        } else {
            return new GrantedAuthoritySid(sid);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public List<AccessEntryResponse> generateAceResponsesByFuzzMatching(AclEntity ae, String nameSeg,
            boolean isCaseSensitive) throws IOException {
        List<AccessEntryResponse> resultsAfterFuzzyMatching = generateAceResponsesByFuzzMatching(getAcl(ae), nameSeg,
                isCaseSensitive);

        Map<Boolean, List<AccessEntryResponse>> collect = resultsAfterFuzzyMatching.stream()
                .collect(Collectors.partitioningBy(user -> user.getSid() instanceof GrantedAuthoritySid));

        Stream<AccessEntryResponse> groupAccessEntryResponseStream = collect.get(true).stream()
                .filter(user -> !StringUtils
                        .equalsIgnoreCase(((GrantedAuthoritySid) user.getSid()).getGrantedAuthority(), ROLE_ADMIN));

        Map<String, AccessEntryResponse> userAccessEntryResponse = collect.get(false).stream().collect(Collectors
                .toMap(user -> ((PrincipalSid) user.getSid()).getPrincipal(), Function.identity(), (p, q) -> p));

        Set<String> normalUsers = userService.retainsNormalUser(userAccessEntryResponse.keySet());

        return Stream
                .concat(groupAccessEntryResponseStream,
                        userAccessEntryResponse.entrySet().stream()
                                .filter(entry -> normalUsers.contains(entry.getKey())).map(Map.Entry::getValue))
                .collect(Collectors.toList());
    }

    private List<AccessEntryResponse> generateAceResponsesByFuzzMatching(Acl acl, String nameSeg,
            boolean isCaseSensitive) {
        if (Objects.isNull(acl)) {
            return Collections.emptyList();
        }

        List<AccessEntryResponse> result = new ArrayList<AccessEntryResponse>();
        for (AccessControlEntry ace : acl.getEntries()) {
            if (StringUtils.isNotEmpty(nameSeg) && !needAdd(nameSeg, isCaseSensitive, getName(ace.getSid()))) {
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

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public Map<String, List<String>> getProjectUsersAndGroups(AclEntity ae) throws IOException {
        Map<String, List<String>> result = Maps.newHashMap();
        // get users
        val userList = new ArrayList<String>();
        userList.addAll(getAllAclSids(ae, MetadataConstants.TYPE_USER));
        userList.addAll(userService.getGlobalAdmin());
        result.put(MetadataConstants.TYPE_USER, userList);

        // get groups
        val userGroupList = new ArrayList<String>();
        userGroupList.addAll(getAllAclSids(ae, MetadataConstants.TYPE_GROUP));
        userGroupList.add(ROLE_ADMIN);
        result.put(MetadataConstants.TYPE_GROUP, userGroupList);
        return result;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public List<String> getAllAclSids(AclEntity ae, String type) {
        Acl acl = getAcl(ae);
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
        if (!record.getOwner().equals(sid))
            return;

        // prevent changing owner's admin permission
        if (BasePermission.ADMINISTRATION.equals(record.getPermission(sid)))
            throw new KylinException(PERMISSION_DENIED, msg.getRevokeAdminPermission());
    }

    private String getUserPermissionInProject(String project, String username) throws IOException {
        return getUserMaximumPermissionWithSourceInProject(project, username).getFirst();
    }

    /**
     *
     * @param project project name
     * @param username user name
     * @return <permission, <from group, group name if not null>>
     */
    private Pair<String, Pair<Boolean, String>> getUserMaximumPermissionWithSourceInProject(String project,
            String username) throws IOException {
        if (isGlobalAdmin(username)) {
            return Pair.newPair(ExternalAclProvider.ADMINISTRATION, Pair.newPair(Boolean.FALSE, null));
        }

        if (hasGlobalAdminGroup(username)) {
            return Pair.newPair(ExternalAclProvider.ADMINISTRATION, Pair.newPair(Boolean.TRUE, ROLE_ADMIN));
        }

        // get user's greater permission between user and groups
        Map<Sid, Integer> projectPermissions = getProjectPermission(project);
        Integer greaterPermissionMask = projectPermissions.get(getSid(username, true));
        String greaterPermissionGroup = null;
        List<String> groups = getGroupsOfUser(username);
        for (String group : groups) {
            if (Objects.nonNull(greaterPermissionMask) && greaterPermissionMask == ADMINISTRATION.getMask()) {
                break;
            }
            Integer groupMask = projectPermissions.get(getSid(group, false));
            Integer compareResultMask = getGreaterPermissionMask(groupMask, greaterPermissionMask);
            // greater permission from group
            if (!compareResultMask.equals(greaterPermissionMask)) {
                greaterPermissionGroup = group;
                greaterPermissionMask = compareResultMask;
            }
        }

        Pair<Boolean, String> groupInfo = Pair.newPair(Boolean.FALSE, null);
        if (Objects.nonNull(greaterPermissionGroup)) {
            groupInfo.setKey(Boolean.TRUE);
            groupInfo.setValue(greaterPermissionGroup);
        }
        return Pair.newPair(ExternalAclProvider.convertToExternalPermission(greaterPermissionMask), groupInfo);
    }

    public Pair<String, Pair<Boolean, String>> getUserNormalPermission(String project, UserDetails user) {
        // get user's greater permission between user and groups
        Map<Sid, Integer> projectPermissions = getProjectPermission(project);
        Integer greaterPermissionMask = projectPermissions.get(getSid(user.getUsername(), true));
        String greaterPermissionGroup = null;
        List<String> groups = user.getAuthorities().stream().map(GrantedAuthority::getAuthority)
                .collect(Collectors.toList());
        for (String group : groups) {
            if (Objects.nonNull(greaterPermissionMask) && greaterPermissionMask == ADMINISTRATION.getMask()) {
                break;
            }
            Integer groupMask = projectPermissions.get(getSid(group, false));
            Integer compareResultMask = getGreaterPermissionMask(groupMask, greaterPermissionMask);
            // greater permission from group
            if (!compareResultMask.equals(greaterPermissionMask)) {
                greaterPermissionGroup = group;
                greaterPermissionMask = compareResultMask;
            }
        }
        Pair<Boolean, String> groupInfo = Pair.newPair(Boolean.FALSE, null);
        if (Objects.nonNull(greaterPermissionGroup)) {
            groupInfo.setKey(Boolean.TRUE);
            groupInfo.setValue(greaterPermissionGroup);
        }
        return Pair.newPair(ExternalAclProvider.convertToExternalPermission(greaterPermissionMask), groupInfo);
    }

    public String getCurrentUserPermissionInProject(String project) throws IOException {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (Objects.nonNull(authentication)) {
            return getUserPermissionInProject(project, authentication.getName());
        }
        return null;
    }

    public String getCurrentNormalUserPermissionInProject(String project) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (Objects.nonNull(authentication)) {
            UserDetails userDetails = userService.loadUserByUsername(authentication.getName());
            return getUserNormalPermission(project, userDetails).getFirst();
        }
        return null;
    }

    private String getGroupPermissionInProject(String project, String groupName) throws IOException {
        checkSid(groupName, false);
        if (ROLE_ADMIN.equals(groupName)) {
            return ExternalAclProvider.ADMINISTRATION;
        }
        Map<Sid, Integer> projectPermissions = getProjectPermission(project);
        int mask = projectPermissions.get(getSid(groupName, false));
        return ExternalAclProvider.convertToExternalPermission(mask);
    }

    private Map<Sid, Integer> getProjectPermission(String project) {
        Map<Sid, Integer> sidWithPermission = new HashMap<>();

        String uuid = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).getUuid();
        AclEntity ae = getAclEntity(AclEntityType.PROJECT_INSTANCE, uuid);
        Acl acl = getAcl(ae);
        if (acl != null && acl.getEntries() != null) {
            List<AccessControlEntry> aces = acl.getEntries();
            for (AccessControlEntry ace : aces) {
                Sid sid = ace.getSid();
                sidWithPermission.put(sid, ace.getPermission().getMask());
            }
        }
        return sidWithPermission;
    }

    public boolean hasProjectPermission(String project, String sid, boolean isPrincipal) {
        Map<Sid, Integer> projectPermissionMap = getProjectPermission(project);
        return projectPermissionMap.containsKey(getSid(sid, isPrincipal));
    }

    public List<String> getGrantedProjectsOfUser(String user) throws IOException {
        return getGrantedProjectsOfUserOrGroup(user, true);
    }

    public List<String> getGrantedProjectsOfUserOrGroup(String name, boolean principal) throws IOException {
        checkSid(name, principal);
        List<ProjectInstance> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects();

        boolean userWithGlobalAdminPermission = principal && (isGlobalAdmin(name) || hasGlobalAdminGroup(name));
        boolean adminGroup = !principal && ROLE_ADMIN.equals(name);
        if (userWithGlobalAdminPermission || adminGroup) {
            return projects.stream().map(ProjectInstance::getName).collect(Collectors.toList());
        }

        List<String> groupsOfUser = new ArrayList<>();
        if (principal) {
            groupsOfUser = getGroupsOfUser(name);
        }

        Set<String> grantedProjects = new HashSet<>();

        for (ProjectInstance project : projects) {
            Map<Sid, Integer> projectPermissionMap = getProjectPermission(project.getName());
            if (projectPermissionMap.containsKey(getSid(name, principal))) {
                grantedProjects.add(project.getName());
                continue;
            }
            // add the project which the user's group granted
            boolean userGroupGranted = groupsOfUser.stream()
                    .anyMatch(group -> projectPermissionMap.containsKey(getSid(group, false)));
            if (userGroupGranted) {
                grantedProjects.add(project.getName());
            }
        }
        return new ArrayList<>(grantedProjects);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public List<SidPermissionWithAclResponse> getUserOrGroupAclPermissions(List<String> projects,
            String userOrGroupName, boolean principal) throws IOException {
        checkSid(userOrGroupName, principal);
        List<SidPermissionWithAclResponse> sidPermissionWithAclResponse = new ArrayList<>();
        for (String project : projects) {
            SidPermissionWithAclResponse permissionWithAclResponse = principal
                    ? getUserPermissionWithAclResponse(project, userOrGroupName)
                    : getGroupPermissionWithAclResponse(project, userOrGroupName);
            sidPermissionWithAclResponse.add(permissionWithAclResponse);
        }
        return sidPermissionWithAclResponse;
    }

    private SidPermissionWithAclResponse getUserPermissionWithAclResponse(String project, String username)
            throws IOException {
        Pair<String, Pair<Boolean, String>> permissionInfo = getUserMaximumPermissionWithSourceInProject(project,
                username);
        if (Boolean.FALSE.equals(permissionInfo.getSecond().getFirst())) {
            List<AclTCRResponse> aclTCRResponses = aclTCRService.getAclTCRResponse(project, username, true, false);
            return new SidPermissionWithAclResponse(project, permissionInfo.getFirst(), aclTCRResponses);
        } else {
            return getGroupPermissionWithAclResponse(project, permissionInfo.getSecond().getSecond());
        }
    }

    private SidPermissionWithAclResponse getGroupPermissionWithAclResponse(String project, String groupName)
            throws IOException {
        String permission = getGroupPermissionInProject(project, groupName);
        List<AclTCRResponse> aclTCRResponses = aclTCRService.getAclTCRResponse(project, groupName, false, false);
        return new SidPermissionWithAclResponse(project, permission, aclTCRResponses);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public Set<String> getProjectAdminUsers(String project) throws IOException {
        MutableAclRecord acl = AclPermissionUtil.getProjectAcl(project);
        Set<String> groupsInProject = AclPermissionUtil.filterGroupsInProject(acl);
        return userService.listUsers().parallelStream().filter(user -> {
            Set<String> userGroupsInProject = user.getAuthorities().stream().map(SimpleGrantedAuthority::getAuthority)
                    .filter(groupsInProject::contains).collect(Collectors.toSet());

            return user.getAuthorities().stream().map(GrantedAuthority::getAuthority).anyMatch(ROLE_ADMIN::equals)
                    || AclPermissionUtil.isSpecificPermissionInProject(user.getUsername(), userGroupsInProject,
                            ADMINISTRATION, acl);
        }).map(ManagedUser::getUsername).collect(Collectors.toSet());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public Set<String> getProjectManagementUsers(String project) throws IOException {
        MutableAclRecord acl = AclPermissionUtil.getProjectAcl(project);
        Set<String> groupsInProject = AclPermissionUtil.filterGroupsInProject(acl);
        AclPermissionUtil.filterGroupsInProject(acl);
        return userService.listUsers().parallelStream().filter(user -> {
            Set<String> userGroupsInProject = user.getAuthorities().stream().map(SimpleGrantedAuthority::getAuthority)
                    .filter(groupsInProject::contains).collect(Collectors.toSet());

            return user.getAuthorities().stream().map(GrantedAuthority::getAuthority).anyMatch(ROLE_ADMIN::equals)
                    || AclPermissionUtil.isSpecificPermissionInProject(user.getUsername(), userGroupsInProject,
                            ADMINISTRATION, acl)
                    || AclPermissionUtil.isSpecificPermissionInProject(user.getUsername(), userGroupsInProject,
                            AclPermission.MANAGEMENT, acl);
        }).map(ManagedUser::getUsername).collect(Collectors.toSet());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public boolean remoteGrantAccess(AclEntity ae, String identifier, Boolean isPrincipal, String permission) {
        AccessGrantEventNotifier notifier = new AccessGrantEventNotifier(UnitOfWork.GLOBAL_UNIT, ae.getId(), identifier,
                isPrincipal, permission);
        return remoteRequest(notifier, null);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public boolean remoteBatchGrantAccess(List<AccessRequest> requests, AclEntity ae) throws JsonProcessingException {
        AccessBatchGrantEventNotifier notifier = new AccessBatchGrantEventNotifier(UnitOfWork.GLOBAL_UNIT, ae.getId(),
                JsonUtil.writeValueAsString(requests));
        return remoteRequest(notifier, null);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public boolean remoteRevokeAccess(AclEntity ae, String name, boolean principal) {
        AccessRevokeEventNotifier notifier = new AccessRevokeEventNotifier(UnitOfWork.GLOBAL_UNIT, ae.getId(), name,
                principal);
        return remoteRequest(notifier, null);
    }

    @Transaction
    public void updateAccessFromRemote(AccessGrantEventNotifier grantNotifier,
            AccessBatchGrantEventNotifier batchGrantNotifier, AccessRevokeEventNotifier revokeNotifier)
            throws IOException {
        if (grantNotifier != null) {
            AclEntity ae = getAclEntity(AclEntityType.PROJECT_INSTANCE, grantNotifier.getEntityId());
            grant(ae, grantNotifier.getIdentifier(), grantNotifier.getIsPrincipal(), grantNotifier.getPermission());
        }
        if (batchGrantNotifier != null) {
            AclEntity ae = getAclEntity(AclEntityType.PROJECT_INSTANCE, batchGrantNotifier.getEntityId());
            List<AccessRequest> accessRequest = JsonUtil.readValue(batchGrantNotifier.getRawAclTCRRequests(),
                    new TypeReference<List<AccessRequest>>() {
                    });
            batchGrant(accessRequest, ae);
        }
        if (revokeNotifier != null) {
            AclEntity ae = getAclEntity(AclEntityType.PROJECT_INSTANCE, revokeNotifier.getEntityId());
            revokeWithSid(ae, revokeNotifier.getName(), revokeNotifier.isPrincipal());
        }
    }

    public List<String> getGroupsOfCurrentUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (Objects.nonNull(authentication)) {
            return getGroupsOfUser(authentication.getName());
        }
        return Lists.newArrayList();
    }

    public Set<String> getGroupsOfExecuteUser(String user) {
        if (Objects.nonNull(user)) {
            return new HashSet<>(getGroupsOfUser(user));
        }
        return Sets.newHashSet();
    }

    private List<String> getGroupsOfUser(String username) {
        ManagedUser user = getManagedUser(username);
        if (Objects.isNull(user)) {
            val msg = MsgPicker.getMsg();
            throw new KylinException(USER_NOT_EXIST, String.format(Locale.ROOT, msg.getUserNotFound(), username));
        }
        List<SimpleGrantedAuthority> authorities = user.getAuthorities();
        return authorities.stream().map(SimpleGrantedAuthority::getAuthority).collect(Collectors.toList());
    }

    private ManagedUser getManagedUser(String username) {
        UserDetails details = userService.loadUserByUsername(username);
        if (details == null)
            return null;
        return (ManagedUser) details;
    }

    private Integer getGreaterPermissionMask(Integer mask1, Integer mask2) {
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
        return 0;
    }

    public void checkGlobalAdmin(String username) throws IOException {
        checkGlobalAdmin(Collections.singletonList(username));
    }

    public void checkGlobalAdmin(List<String> usernames) throws IOException {
        if (userService.containsGlobalAdmin(new HashSet<>(usernames))) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getChangeGlobaladmin());
        }
    }

    public void checkDefaultAdmin(String username, boolean isDefaultAdminHasRight) {
        if (StringUtils.equalsIgnoreCase(username, KylinUserService.SUPER_ADMIN)) {
            String currentUser = AclPermissionUtil.getCurrentUsername();
            if (!isDefaultAdminHasRight || !StringUtils.equalsIgnoreCase(KylinUserService.SUPER_ADMIN, currentUser)) {
                throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getChangeDegaultadmin());
            }
        }
    }

    public void checkAccessRequestList(List<AccessRequest> requests) throws IOException {
        checkSid(requests);
        List<String> users = requests.stream().filter(AccessRequest::isPrincipal).map(AccessRequest::getSid)
                .collect(Collectors.toList());
        checkGlobalAdmin(users);
    }

    public void checkSid(List<AccessRequest> requests) throws IOException {
        if (CollectionUtils.isEmpty(requests)) {
            return;
        }
        for (AccessRequest r : requests) {
            checkSid(r.getSid(), r.isPrincipal());
        }
    }

    public void checkSidNotEmpty(String sid, boolean principal) {
        if (StringUtils.isEmpty(sid)) {
            if (principal) {
                throw new KylinException(EMPTY_USER_NAME, MsgPicker.getMsg().getEmptySid());
            } else {
                throw new KylinException(EMPTY_USERGROUP_NAME, MsgPicker.getMsg().getEmptySid());
            }
        }
    }

    public void checkSid(String sid, boolean principal) throws IOException {
        checkSidNotEmpty(sid, principal);
        if (principal && !userService.userExists(sid)) {
            throw new KylinException(PERMISSION_DENIED,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getOperationFailedByUserNotExist(), sid));
        }
        if (!principal && !userGroupService.exists(sid)) {
            throw new KylinException(PERMISSION_DENIED,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getOperationFailedByGroupNotExist(), sid));
        }
    }

    public boolean isGlobalAdmin(String username) throws IOException {
        Set<String> adminUsers = userService.getGlobalAdmin();
        return adminUsers.contains(username);
    }

    public boolean hasGlobalAdminGroup(String username) {
        List<String> groups = getGroupsOfUser(username);
        return groups.contains(ROLE_ADMIN);
    }

}
