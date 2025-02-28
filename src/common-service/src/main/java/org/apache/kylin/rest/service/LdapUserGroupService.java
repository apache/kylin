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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.naming.directory.SearchControls;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.usergroup.UserGroup;
import org.apache.kylin.rest.response.UserGroupResponseKI;
import org.apache.kylin.rest.security.AdminUserSyncEventNotifier;
import org.apache.kylin.tool.util.LdapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.ldap.control.PagedResultsDirContextProcessor;
import org.springframework.ldap.core.ContextMapper;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.support.SingleContextSource;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;

public class LdapUserGroupService extends NUserGroupService {

    private static final Logger logger = LoggerFactory.getLogger(LdapUserGroupService.class);

    private static final String LDAP_GROUPS = "ldap_groups";

    private static final String SKIPPED_LDAP = "skipped-ldap";

    private static final Cache<String, Set<String>> ldapGroupsCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    private static final Cache<String, List<ManagedUser>> ldapGroupsMembersCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    private static final Cache<String, List<String>> ldapGroupsAndMembersCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    @Autowired
    @Qualifier("ldapTemplate")
    private SpringSecurityLdapTemplate ldapTemplate;

    @Autowired
    @Qualifier("userService")
    private LdapUserService ldapUserService;

    @Autowired
    private SearchControls searchControls;

    @Override
    public void addGroup(String name) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getGroupEditNotAllowed());
    }

    @Override
    public void deleteGroup(String name) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getGroupEditNotAllowed());
    }

    @Override
    public void modifyGroupUsers(String groupName, List<String> users) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getGroupEditNotAllowed());
    }

    @Override
    public List<String> getAllUserGroups() {
        Set<String> allGroups = ldapGroupsCache.getIfPresent(LDAP_GROUPS);
        if (allGroups == null || allGroups.isEmpty()) {
            logger.info("Can not get groups from cache, ask ldap instead.");
            String ldapGroupSearchBase = KylinConfig.getInstanceFromEnv().getLDAPGroupSearchBase();
            String ldapGroupSearchFilter = KapConfig.getInstanceFromEnv().getLDAPGroupSearchFilter();
            String ldapGroupIDAttr = KapConfig.getInstanceFromEnv().getLDAPGroupIDAttr();
            Integer maxPageSize = KapConfig.getInstanceFromEnv().getLDAPMaxPageSize();
            logger.info(
                    "ldap group search config, base: {}, filter: {}, identifier attribute: {}, member search filter: {}, member identifier: {}, maxPageSize: {}",
                    ldapGroupSearchBase, ldapGroupSearchFilter, ldapGroupIDAttr,
                    KapConfig.getInstanceFromEnv().getLDAPGroupMemberSearchFilter(),
                    KapConfig.getInstanceFromEnv().getLDAPGroupMemberAttr(), maxPageSize);

            final PagedResultsDirContextProcessor processor = new PagedResultsDirContextProcessor(maxPageSize);

            ContextMapper<String> contextMapper = ctx -> {
                DirContextAdapter adapter = (DirContextAdapter) ctx;
                return adapter.getAttributes().get(ldapGroupIDAttr).get().toString();
            };

            allGroups = SingleContextSource.doWithSingleContext(ldapTemplate.getContextSource(), operations -> {
                Set<String> set = new HashSet<>();
                do {
                    set.addAll(operations.search(ldapGroupSearchBase, ldapGroupSearchFilter, searchControls,
                            contextMapper, processor));
                } while (processor.hasMore());
                return set;
            });

            ldapGroupsCache.put(LDAP_GROUPS, allGroups);
        }
        logger.info("Get all groups size: {}", allGroups.size());
        return Collections.unmodifiableList(Lists.newArrayList(allGroups));
    }

    @Override
    public List<UserGroup> listUserGroups() {
        return getUserGroupSpecialUuid();
    }

    @Override
    public Map<String, List<String>> getUserAndUserGroup() {
        Map<String, List<String>> result = Maps.newHashMap();
        for (String group : getAllUserGroups()) {
            result.put(group, getGroupUsernameList(group));
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public List<UserGroup> getUserGroupsFilterByGroupName(String userGroupName) {
        aclEvaluate.checkIsGlobalAdmin();
        return listUserGroups().stream()
                .filter(userGroup -> StringUtils.isEmpty(userGroupName) || userGroup.getGroupName()
                        .toUpperCase(Locale.ROOT).contains(userGroupName.toUpperCase(Locale.ROOT)))
                .collect(Collectors.toList());
    }

    @Override
    public List<ManagedUser> getGroupMembersByName(String name) {
        List<ManagedUser> members = ldapGroupsMembersCache.getIfPresent(name);
        if (null == members) {
            logger.info("Can not get the group {}'s all members from cache, ask ldap instead.", name);
            members = new ArrayList<>();
            List<String> usernameList = getGroupUsernameList(name);
            for (String username : usernameList) {
                boolean userExists = userService.userExists(username);
                if (userExists) {//guard groups may have ou or groups
                    ManagedUser ldapUser = new ManagedUser(username, SKIPPED_LDAP, false,
                            Lists.<GrantedAuthority> newArrayList());
                    ldapUserService.completeUserInfoInternal(ldapUser);
                    members.add(ldapUser);
                }
            }
            ldapGroupsMembersCache.put(name, Collections.unmodifiableList(members));
        }
        return members;
    }

    @Override
    public List<UserGroupResponseKI> getUserGroupResponse(List<UserGroup> userGroups) throws IOException {
        List<UserGroupResponseKI> result = new ArrayList<>();
        for (UserGroup group : userGroups) {
            Set<String> groupMembers = new TreeSet<>(getGroupUsernameList(group.getGroupName()));
            result.add(new UserGroupResponseKI(group.getUuid(), group.getGroupName(), groupMembers));
        }
        return result;
    }

    private List<String> getGroupUsernameList(String name) {
        List<String> users = ldapGroupsAndMembersCache.getIfPresent(name);
        if (null == users) {
            users = new ArrayList<>();
            Set<String> ldapUserDNs = LdapUtils.getAllGroupMembers(ldapTemplate, name).stream()
                    .filter(StringUtils::isNotBlank).collect(Collectors.toSet());

            Map<String, String> dnMapperMap = ldapUserService.getDnMapperMap();

            for (String u : ldapUserDNs) {
                Optional.ofNullable(dnMapperMap.get(u)).ifPresent(users::add);
            }
            List<String> userList = Collections.unmodifiableList(users);
            syncAdminUser(name, userList);
            ldapGroupsAndMembersCache.put(name, userList);
        }
        return users;
    }

    private void syncAdminUser(String groupName, List<String> userList) {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        if (conf.getLDAPAdminRole().equalsIgnoreCase(groupName)) {
            EventBusFactory.getInstance().postSync(new AdminUserSyncEventNotifier(userList, true));
        }
    }

    @Override
    public String getGroupNameByUuid(String uuid) {
        return uuid;
    }

    @Override
    public String getUuidByGroupName(String groupName) {
        return groupName;
    }

    @Override
    public boolean exists(String name) {
        return getAllUserGroups().contains(name);
    }

    @Override
    public Set<String> listUserGroups(String username) {
        return getAllUserGroups().stream()
                .filter(group -> getGroupMembersByName(group).stream()
                        .anyMatch(user -> StringUtils.equalsIgnoreCase(username, user.getUsername())))
                .collect(Collectors.toSet());
    }
}
