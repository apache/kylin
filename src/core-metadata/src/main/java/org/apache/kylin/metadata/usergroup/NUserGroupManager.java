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

package org.apache.kylin.metadata.usergroup;

import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_USERGROUP_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.USERGROUP_NOT_EXIST;
import static org.apache.kylin.common.persistence.ResourceStore.USER_GROUP_ROOT;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class NUserGroupManager {
    private static final Logger logger = LoggerFactory.getLogger(NUserGroupManager.class);

    public static NUserGroupManager getInstance(KylinConfig config) {
        return config.getManager(NUserGroupManager.class);
    }

    private KylinConfig config;
    private CachedCrudAssist<UserGroup> crud;

    // called by reflection
    static NUserGroupManager newInstance(KylinConfig config) {
        return new NUserGroupManager(config);
    }

    public NUserGroupManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NUserGroupManager with KylinConfig Id: {}", System.identityHashCode(config));
        this.config = config;
        this.crud = new CachedCrudAssist<UserGroup>(getStore(), USER_GROUP_ROOT, "", UserGroup.class) {
            @Override
            protected UserGroup initEntityAfterReload(UserGroup userGroup, String resourceName) {
                return userGroup;
            }
        };

    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public List<String> getAllGroupNames() {
        return ImmutableList.copyOf(crud.listAll().stream().map(UserGroup::getGroupName).collect(Collectors.toList()));
    }

    public List<UserGroup> getAllGroups() {
        return ImmutableList.copyOf(crud.listAll());
    }

    public boolean exists(String name) {
        return getAllGroupNames().contains(name);
    }

    public UserGroup copyForWrite(UserGroup userGroup) {
        return crud.copyForWrite(userGroup);
    }

    @VisibleForTesting
    public String getRealUserGroupByName(String groupName) {
        for (String name : getAllGroupNames()) {
            if (StringUtils.equalsIgnoreCase(name, groupName)) {
                return name;
            }
        }
        return null;
    }

    public List<String> getRealUserGroupByNames(List<String> names) {
        List<String> result = Lists.newArrayList();
        List<String> allGroups = getAllGroupNames();
        for (String tmp : names) {
            for (String name : allGroups) {
                if (StringUtils.endsWithIgnoreCase(tmp, name)) {
                    result.add(tmp);
                    break;
                }
            }
        }
        return result;
    }

    public void batchAdd(List<String> names) {
        List<String> realGroups = getRealUserGroupByNames(names);
        if (realGroups.size() > 0) {
            throw new KylinException(DUPLICATE_USERGROUP_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getUserGroupExist(), String.join(",", realGroups)));
        }
        for (String name : names) {
            UserGroup userGroup = new UserGroup(name);
            crud.save(userGroup);
        }
    }

    public void add(String name) {
        String realGroupName = getRealUserGroupByName(name);
        if (StringUtils.isNotEmpty(realGroupName)) {
            throw new KylinException(DUPLICATE_USERGROUP_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getUserGroupExist(), realGroupName));
        }
        UserGroup userGroup = new UserGroup(name);
        crud.save(userGroup);
    }

    public void delete(String name) {
        String realGroupName = getRealUserGroupByName(name);
        if (StringUtils.isEmpty(realGroupName)) {
            throw new KylinException(USERGROUP_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getUserGroupNotExist(), name));
        }
        crud.delete(realGroupName);
    }
}
