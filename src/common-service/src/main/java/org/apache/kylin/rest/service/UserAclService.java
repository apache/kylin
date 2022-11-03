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

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.CaseInsensitiveStringSet;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.GlobalAccessRequest;
import org.apache.kylin.rest.request.GlobalBatchAccessRequest;
import org.apache.kylin.rest.response.UserAccessEntryResponse;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.security.AdminUserSyncEventNotifier;
import org.apache.kylin.rest.security.ExternalAclProvider;
import org.apache.kylin.rest.security.UserAcl;
import org.apache.kylin.rest.security.UserAclManager;
import org.apache.kylin.tool.upgrade.UpdateUserAclTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.epoch.EpochManager;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("userAclService")
public class UserAclService extends BasicService implements UserAclServiceSupporter {

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    public boolean hasUserAclPermission(String sid, Permission permission) {
        val userAcl = getManager(UserAclManager.class).get(sid);
        return !Objects.isNull(userAcl) && CollectionUtils.isNotEmpty(userAcl.getPermissionMasks())
                && userAcl.getPermissionMasks().contains(permission.getMask());
    }

    @Override
    @SneakyThrows(IOException.class)
    public boolean hasUserAclPermissionInProject(String project) {
        val userName = getLoginUsername();
        return userService.isGlobalAdmin(userName) && hasUserAclPermissionInProject(userName, project);
    }

    public boolean hasUserAclPermissionInProject(String sid, String project) {
        val userAcl = getManager(UserAclManager.class).get(sid);
        return !Objects.isNull(userAcl) && CollectionUtils.isNotEmpty(userAcl.getDataQueryProjects())
                && userAcl.getDataQueryProjects().contains(project);
    }

    private void checkAclPermission(String sid, String permissionType) {
        Preconditions.checkArgument(ExternalAclProvider.DATA_QUERY.equalsIgnoreCase(permissionType),
                "unknown PermissionType " + permissionType);
        if (isSuperAdmin(sid)) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getModifyPermissionOfSuperAdminFailed());
        }
        checkAdminUser(sid);
        if (sid.equalsIgnoreCase(getLoginUsername())) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getModifyOwnPermissionFailed());
        }
    }

    private void checkLoginUserPermission() {
        if (!canAdminUserQuery()) {
            throw new KylinException(PERMISSION_DENIED,
                    MsgPicker.getMsg().getGrantPermissionFailedByIllegalAuthorizingUser());
        }
    }

    private void checkLoginUserPermissionInPrj(String project) {
        if (!canAdminUserQuery() && !hasUserAclPermissionInProject(project)) {
            throw new KylinException(PERMISSION_DENIED,
                    MsgPicker.getMsg().getGrantPermissionFailedByIllegalAuthorizingUser());
        }
    }

    @Transaction
    public void grantUserAclPermission(GlobalBatchAccessRequest batchAccessRequest, String permissionType) {
        batchAccessRequest.getUsernameList().forEach(sid -> grantUserAclPermission(sid, permissionType));
    }

    @Transaction
    public void grantUserAclPermission(GlobalAccessRequest accessRequest, String permissionType) {
        grantUserAclPermission(accessRequest.getUsername(), permissionType);
    }

    @Transaction
    public void grantUserAclPermission(String sid, String permissionType) {
        checkAclPermission(sid, permissionType);
        checkLoginUserPermission();
        getManager(UserAclManager.class).addPermission(sid,
                AclPermissionFactory.getPermission(permissionType.toUpperCase(Locale.ROOT)));
    }

    @Transaction
    public void addProjectToUserAcl(GlobalAccessRequest accessRequest, String permissionType) {
        checkAclPermission(accessRequest.getUsername(), permissionType);
        checkLoginUserPermissionInPrj(accessRequest.getProject());
        getManager(UserAclManager.class).addDataQueryProject(accessRequest.getUsername(), accessRequest.getProject());
    }

    @Transaction
    public void revokeUserAclPermission(GlobalBatchAccessRequest batchAccessRequest, String permissionType) {
        batchAccessRequest.getUsernameList().forEach(sid -> revokeUserAclPermission(sid, permissionType));
    }

    @Transaction
    public void revokeUserAclPermission(GlobalAccessRequest accessRequest, String permissionType) {
        revokeUserAclPermission(accessRequest.getUsername(), permissionType);
    }

    @Transaction
    public void revokeUserAclPermission(String sid, String permissionType) {
        checkAclPermission(sid, permissionType);
        checkLoginUserPermission();
        val manager = getManager(UserAclManager.class);
        manager.deletePermission(sid, AclPermissionFactory.getPermission(permissionType.toUpperCase(Locale.ROOT)));
        if (!manager.exists(sid)) {
            manager.addPermission(sid, Collections.emptySet());
        }
    }

    @Transaction
    public void deleteProjectFromUserAcl(GlobalAccessRequest accessRequest, String permissionType) {
        checkAclPermission(accessRequest.getUsername(), permissionType);
        checkLoginUserPermissionInPrj(accessRequest.getProject());
        getManager(UserAclManager.class).deleteDataQueryProject(accessRequest.getUsername(),
                accessRequest.getProject());
    }

    public List<UserAccessEntryResponse> listUserAcl() {

        final List<String> adminUsers = new ArrayList<>();
        try {
            adminUsers.addAll(userService.listAdminUsers());
        } catch (IOException e) {
            log.error("listAdminUsers error", e);
            return Collections.emptyList();
        }
        List<UserAcl> userAclList = getManager(UserAclManager.class).listUserAcl().stream()
                .filter(u -> adminUsers.stream().anyMatch(adminUser -> adminUser.equalsIgnoreCase(u.getUsername())))
                .collect(Collectors.toList());
        final Map<String, UserAccessEntryResponse> responseMap = userAclList.stream()
                .collect(Collectors.toMap(UserAcl::getUsername, this::createUserAccessEntryResponse));

        Set<String> superAdminUserSet = new CaseInsensitiveStringSet(new HashSet<>(userService.listSuperAdminUsers()));
        superAdminUserSet.forEach(superAdminUser -> responseMap.put(superAdminUser,
                createUserAccessEntryResponse(new UserAcl(superAdminUser, Sets.newHashSet(AclPermission.DATA_QUERY)))));
        return responseMap.values().stream().collect(Collectors.toList());
    }

    private UserAccessEntryResponse createUserAccessEntryResponse(UserAcl userAcl) {
        List<String> permissions = CollectionUtils.isEmpty(userAcl.getPermissionMasks()) ? Collections.emptyList()
                : userAcl.getPermissionMasks().stream().map(ExternalAclProvider::convertToExternalPermission)
                .collect(Collectors.toList());
        return new UserAccessEntryResponse(userAcl.getUsername(), permissions, userAcl.getDataQueryProjects());
    }

    /**
     * check authorized target user
     * @param sid
     */
    @SneakyThrows(IOException.class)
    private void checkAdminUser(String sid) {
        if (StringUtils.isEmpty(sid)) {
            throw new KylinException(EMPTY_USER_NAME, MsgPicker.getMsg().getEmptySid());
        }
        if (!userService.userExists(sid)) {
            throw new KylinException(PERMISSION_DENIED,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getOperationFailedByUserNotExist(), sid));
        }
        if (!userService.isGlobalAdmin(sid)) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getGrantPermissionFailedByNonSystemAdmin());
        }
    }

    public boolean isSuperAdmin(String username) {
        val superAdminList = userService.listSuperAdminUsers();
        if (CollectionUtils.isEmpty(superAdminList)) {
            return false;
        }
        return superAdminList.stream().anyMatch(superAdmin -> superAdmin.equalsIgnoreCase(username));
    }

    /**
     * Is the current login user is super admin, or system admin who has query permission
     * @return
     */
    @Override
    public boolean canAdminUserQuery() {
        String username = getLoginUsername();
        return canAdminUserQuery(username);
    }

    @SneakyThrows(IOException.class)
    public boolean canAdminUserQuery(String username) {
        return (isSuperAdmin(username)
                || (userService.isGlobalAdmin(username) && hasUserAclPermission(username, AclPermission.DATA_QUERY)));
    }

    private String getLoginUsername() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return authentication.getName();
    }

    @Override
    @SneakyThrows(IOException.class)
    public void checkAdminUserPermission(String projectName) {
        String username = getLoginUsername();
        if (userService.isGlobalAdmin(username) && !hasUserAclPermission(username, AclPermission.DATA_QUERY)
                && !hasUserAclPermissionInProject(username, projectName)) {
            throw new AccessDeniedException(StringUtils.EMPTY);
        }
    }

    @Transaction
    public void updateUserAclPermission(UserDetails user, Permission permission) {
        UserAclManager userAclManager = getManager(UserAclManager.class);
        if (!isRoleAdmin(user) && userAclManager.exists(user.getUsername())) {
            userAclManager.delete(user.getUsername());
            return;
        }
        if (isRoleAdmin(user) && !userAclManager.exists(user.getUsername())) {
            Set<Permission> permissions = KylinConfig.getInstanceFromEnv().isDataPermissionDefaultEnabled()
                    ? Sets.newHashSet(permission)
                    : Collections.emptySet();
            userAclManager.addPermission(user.getUsername(), permissions);
        }
    }

    private boolean isRoleAdmin(UserDetails user) {
        return user.getAuthorities().contains(new SimpleGrantedAuthority(Constant.ROLE_ADMIN));
    }

    @Transaction
    public void deleteUserAcl(String userName) {
        getManager(UserAclManager.class).delete(userName);
    }

    public void remoteSyncAdminUserAcl(AdminUserSyncEventNotifier eventNotifier) {
        eventNotifier.setProject(UnitOfWork.GLOBAL_UNIT);
        remoteRequest(eventNotifier, StringUtils.EMPTY);
    }

    @SneakyThrows(IOException.class)
    public void syncAdminUserAcl() {
        val config = KylinConfig.getInstanceFromEnv();
        if (UpdateUserAclTool.isCustomProfile()) {
            // invoke the AdminUserAspect
            userService.listAdminUsers();
        } else if ("ldap".equals(config.getSecurityProfile())) {
            syncSuperAdminUserAcl();
            syncAdminUserAcl(userService.listAdminUsers(), true);
        } else {
            syncSuperAdminUserAcl();
        }
    }

    public void syncSuperAdminUserAcl() {
        List<String> superAdminUserList = userService.listSuperAdminUsers();
        if (CollectionUtils.isEmpty(superAdminUserList)
                || !EpochManager.getInstance().checkEpochOwner(UnitOfWork.GLOBAL_UNIT)) {
            return;
        }
        if (superAdminUserList.stream().allMatch(su -> hasUserAclPermission(su, AclPermission.DATA_QUERY))) {
            return;
        }
        // add query permission
        if (CollectionUtils.isNotEmpty(superAdminUserList)) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                val config = KylinConfig.getInstanceFromEnv();
                UserAclManager manager = UserAclManager.getInstance(config);
                superAdminUserList.stream().filter(su -> !hasUserAclPermission(su, AclPermission.DATA_QUERY))
                        .forEach(manager::add);
                return null;
            }, UnitOfWork.GLOBAL_UNIT, 1);
        }
    }

    /**
     * sync the admin users from api to metadata
     * @param apiAdminUserList
     * @param useEmptyPermission
     */
    public void syncAdminUserAcl(List<String> apiAdminUserList, boolean useEmptyPermission) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val userAclManager = UserAclManager.getInstance(kylinConfig);
        val dbAdminUserList = userAclManager.listAclUsernames();
        if (CollectionUtils.isEmpty(apiAdminUserList)
                || !EpochManager.getInstance().checkEpochOwner(UnitOfWork.GLOBAL_UNIT)) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            // add query permission
            val adminUserAclAddList = getIntersect(apiAdminUserList, dbAdminUserList);
            if (CollectionUtils.isNotEmpty(adminUserAclAddList)) {
                val config = KylinConfig.getInstanceFromEnv();
                UserAclManager manager = UserAclManager.getInstance(config);
                log.info("adminUserAclAddList:{}", adminUserAclAddList);
                adminUserAclAddList.stream().filter(adminUser -> !manager.exists(adminUser)).forEach(adminUser -> {
                    if (isSuperAdmin(adminUser) || !useEmptyPermission || config.isDataPermissionDefaultEnabled()) {
                        manager.add(adminUser);
                    } else {
                        manager.addPermission(adminUser, Collections.emptySet());
                    }
                });
            }
            // remove query permission
            val adminUserAclRemoveList = getIntersect(dbAdminUserList, apiAdminUserList);
            if (CollectionUtils.isNotEmpty(adminUserAclRemoveList)) {
                UserAclManager manager = UserAclManager.getInstance(KylinConfig.getInstanceFromEnv());
                log.info("adminUserAclRemoveList:{}", adminUserAclRemoveList);
                adminUserAclRemoveList.stream().forEach(adminUser -> manager.delete(adminUser));
            }
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    private List<String> getIntersect(List<String> sourceList, List<String> destList) {
        val copyOfSourceList = new ArrayList<String>();
        copyOfSourceList.addAll(sourceList);
        copyOfSourceList.removeAll(destList);
        return copyOfSourceList;
    }

}
