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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.naming.directory.SearchControls;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.CaseInsensitiveStringMap;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.metadata.user.ManagedUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.ldap.control.PagedResultsDirContextProcessor;
import org.springframework.ldap.core.ContextMapper;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.support.SingleContextSource;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;
import org.springframework.security.ldap.userdetails.LdapUserDetailsService;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class LdapUserService implements UserService {

    private static final Logger logger = LoggerFactory.getLogger(LdapUserService.class);

    private static final String LDAP_USERS = "ldap_users";

    private static final String SKIPPED_LDAP = "skipped-ldap";

    private static final String LDAP_VALID_DN_MAP_KEY = "ldap_valid_dn_map_key";

    private static final AtomicBoolean LOAD_TASK_STATUS = new AtomicBoolean(Boolean.FALSE);

    private static final ThreadPoolExecutor LOAD_TASK_POOL = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1), Executors.defaultThreadFactory(), (r, e) -> {
            });

    private static final com.google.common.cache.Cache<String, Map<String, ManagedUser>> ldapUsersCache = CacheBuilder
            .newBuilder().maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    private static final com.google.common.cache.Cache<String, Map<String, String>> LDAP_VALID_DN_MAP_CACHE = CacheBuilder
            .newBuilder().maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    @Autowired
    @Qualifier("ldapTemplate")
    private SpringSecurityLdapTemplate ldapTemplate;

    @Autowired
    @Qualifier("ldapUserDetailsService")
    private LdapUserDetailsService ldapUserDetailsService;

    @Autowired
    @Qualifier("userGroupService")
    private LdapUserGroupService userGroupService;

    @Autowired
    private SearchControls searchControls;

    @Override
    public void createUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getUserEditNotAllowed());
    }

    @Override
    public void updateUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getUserEditNotAllowed());
    }

    @Override
    public void deleteUser(String s) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getUserEditNotAllowed());
    }

    @Override
    public void changePassword(String s, String s1) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getUserEditNotAllowed());
    }

    @Override
    public boolean userExists(String username) {
        Map<String, ManagedUser> managedUserMap = this.getLDAPUsersCache();
        if (Objects.nonNull(managedUserMap)) {
            return managedUserMap.containsKey(username);
        }
        UserDetails ldapUser = ldapUserDetailsService.loadUserByUsername(username);
        this.asyncLoadCacheData();
        return Objects.nonNull(ldapUser);
    }

    @Override
    public UserDetails loadUserByUsername(String username) {
        Map<String, ManagedUser> managedUserMap = this.getLDAPUsersCache();
        if (Objects.nonNull(managedUserMap)) {
            for (Map.Entry<String, ManagedUser> entry : managedUserMap.entrySet()) {
                if (StringUtils.equalsIgnoreCase(username, entry.getKey())) {
                    return entry.getValue();
                }
            }
            throw new UsernameNotFoundException(
                    String.format(Locale.ROOT, MsgPicker.getMsg().getUserNotFound(), username));
        } else {
            UserDetails ldapUser = ldapUserDetailsService.loadUserByUsername(username);
            this.asyncLoadCacheData();
            if (Objects.isNull(ldapUser)) {
                String msg = String.format(Locale.ROOT, MsgPicker.getMsg().getUserNotFound(), username);
                throw new UsernameNotFoundException(msg);
            }
            return new ManagedUser(ldapUser.getUsername(), SKIPPED_LDAP, false, ldapUser.getAuthorities());
        }
    }

    @Override
    public List<ManagedUser> listUsers() {
        Map<String, ManagedUser> allUsers = ldapUsersCache.getIfPresent(LDAP_USERS);
        if (CollectionUtils.isEmpty(allUsers)) {
            logger.info("Failed to read users from cache, reload from ldap server.");
            allUsers = new CaseInsensitiveStringMap<>();
            Set<String> ldapUsers = getAllUsers();
            for (String user : ldapUsers) {
                ManagedUser ldapUser = new ManagedUser(user, SKIPPED_LDAP, false, Lists.newArrayList());
                try {
                    completeUserInfoInternal(ldapUser);
                    allUsers.put(user, ldapUser);
                } catch (IncorrectResultSizeDataAccessException e) {
                    logger.warn("Complete user {} info exception", ldapUser.getUsername(), e);
                }
            }
            ldapUsersCache.put(LDAP_USERS, Preconditions.checkNotNull(allUsers,
                    "Failed to load users from ldap server, something went wrong."));
            logger.info("Get all users size: {}", allUsers.size());
        }
        return Collections.unmodifiableList(new ArrayList<>(allUsers.values()));
    }

    @Override
    public List<String> listAdminUsers() throws IOException {
        List<String> adminUsers = new ArrayList<>();
        for (ManagedUser user : userGroupService
                .getGroupMembersByName(KylinConfig.getInstanceFromEnv().getLDAPAdminRole())) {
            adminUsers.add(user.getUsername());
        }
        return Collections.unmodifiableList(adminUsers);
    }

    @Override
    public void completeUserInfo(ManagedUser user) {
        //do nothing
    }

    public void completeUserInfoInternal(ManagedUser user) {
        Map<String, List<String>> userAndUserGroup = userGroupService.getUserAndUserGroup();
        for (Map.Entry<String, List<String>> entry : userAndUserGroup.entrySet()) {
            String groupName = entry.getKey();
            Set<String> userSet = new HashSet<>(entry.getValue());
            if (!userSet.contains(user.getUsername())) {
                continue;
            }
            if (groupName.equals(KylinConfig.getInstanceFromEnv().getLDAPAdminRole())) {
                user.addAuthorities(groupName);
                user.addAuthorities(Constant.ROLE_ADMIN);
            } else {
                user.addAuthorities(groupName);
            }
        }
    }

    public void onUserAuthenticated(String username) {
        if (!userExists(username)) {
            logger.info("User {} not exists, invalidate cache {}.", username, LDAP_USERS);
            ldapUsersCache.invalidate(LDAP_USERS);
        }
    }

    private Set<String> getAllUsers() {
        Map<String, String> userDnMap = getAllValidUserDnMap();
        LDAP_VALID_DN_MAP_CACHE.put(LDAP_VALID_DN_MAP_KEY, ImmutableMap.copyOf(userDnMap));
        return new HashSet<>(userDnMap.values());
    }

    public Map<String, String> getDnMapperMap() {
        Map<String, String> map = LDAP_VALID_DN_MAP_CACHE.getIfPresent(LDAP_VALID_DN_MAP_KEY);
        if (null == map) {
            map = ImmutableMap.copyOf(getAllValidUserDnMap());
            LDAP_VALID_DN_MAP_CACHE.put(LDAP_VALID_DN_MAP_KEY, map);
        }
        return map;
    }

    private Map<String, String> getAllValidUserDnMap() {
        String ldapUserSearchBase = KylinConfig.getInstanceFromEnv().getLDAPUserSearchBase();
        String ldapUserSearchFilter = KapConfig.getInstanceFromEnv().getLDAPUserSearchFilter();
        String ldapUserIDAttr = KapConfig.getInstanceFromEnv().getLDAPUserIDAttr();
        Integer maxPageSize = KapConfig.getInstanceFromEnv().getLDAPMaxPageSize();
        logger.info("ldap user search config, base: {}, filter: {}, identifier attribute: {}, maxPageSize: {}",
                ldapUserSearchBase, ldapUserSearchFilter, ldapUserIDAttr, maxPageSize);

        final PagedResultsDirContextProcessor processor = new PagedResultsDirContextProcessor(maxPageSize);

        ContextMapper<Pair<String, String>> contextMapper = ctx -> {
            DirContextAdapter adapter = (DirContextAdapter) ctx;
            return Pair.newPair(adapter.getNameInNamespace(),
                    adapter.getAttributes().get(ldapUserIDAttr).get().toString());
        };

        List<Pair<String, String>> pairs = SingleContextSource.doWithSingleContext(ldapTemplate.getContextSource(),
                operations -> {
                    List<Pair<String, String>> pairList = new ArrayList<>();
                    do {
                        pairList.addAll(operations.search(ldapUserSearchBase, ldapUserSearchFilter, searchControls,
                                contextMapper, processor));
                    } while (processor.hasMore());
                    return pairList;
                });

        Map<String, String> resultMap = pairs.stream().collect(Collectors.groupingBy(Pair::getSecond)).entrySet()
                .stream().filter(e -> 1 == e.getValue().size()).map(Map.Entry::getValue).map(list -> list.get(0))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        logger.info("LDAP user info load success");
        return resultMap;
    }

    private Map<String, ManagedUser> getLDAPUsersCache() {
        return Optional.ofNullable(ldapUsersCache.getIfPresent(LDAP_USERS)).map(Collections::unmodifiableMap)
                .orElse(null);
    }

    private void asyncLoadCacheData() {
        if (null != this.getLDAPUsersCache() || LOAD_TASK_STATUS.get()) {
            return;
        }
        Runnable runnable = () -> {
            if (null != this.getLDAPUsersCache()) {
                return;
            }
            if (!LOAD_TASK_STATUS.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
                return;
            }
            try {
                this.listUsers();
            } catch (Exception e) {
                logger.error("Failed to refresh cache asynchronously", e);
            } finally {
                LOAD_TASK_STATUS.set(Boolean.FALSE);
            }
        };
        try {
            LOAD_TASK_POOL.execute(runnable);
        } catch (Exception e) {
            logger.error("load user cache task error", e);
        }
    }
}
