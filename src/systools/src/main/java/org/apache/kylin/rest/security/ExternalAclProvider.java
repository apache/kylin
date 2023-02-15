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

package org.apache.kylin.rest.security;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.model.Permission;

/**
 */
public abstract class ExternalAclProvider {

    public static ExternalAclProvider getInstance() {
        return Singletons.getInstance(ExternalAclProvider.class, clz -> {
            ExternalAclProvider singleton = null;
            String cls = KylinConfig.getInstanceFromEnv().getExternalAclProvider();
            if (!StringUtils.isBlank(cls)) {
                singleton = (ExternalAclProvider) ClassUtil.newInstance(cls);
                singleton.init();
            }
            return singleton;
        });
    }

    // ============================================================================

    public static final String ADMINISTRATION = "ADMIN";
    public static final String MANAGEMENT = "MANAGEMENT";
    public static final String OPERATION = "OPERATION";
    public static final String READ = "QUERY";
    public static final String EMPTY = "EMPTY";
    public static final String DATA_QUERY = "DATA_QUERY";

    // used by ranger ExternalAclProvider
    public static String convertToExternalPermission(Permission p) {
        String permString;
        if (BasePermission.ADMINISTRATION.equals(p)) {
            permString = ADMINISTRATION;
        } else if (AclPermission.MANAGEMENT.equals(p)) {
            permString = MANAGEMENT;
        } else if (AclPermission.OPERATION.equals(p)) {
            permString = OPERATION;
        } else if (BasePermission.READ.equals(p)) {
            permString = READ;
        } else if (AclPermission.DATA_QUERY.equals(p)) {
            permString = DATA_QUERY;
        } else {
            permString = p.getPattern();
        }
        return permString;
    }

    public static void checkExternalPermission(String permission) {
        if (StringUtils.isBlank(permission)) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getEmptyPermission());
        }
        if (ADMINISTRATION.equalsIgnoreCase(permission) || MANAGEMENT.equalsIgnoreCase(permission)
                || OPERATION.equalsIgnoreCase(permission) || READ.equalsIgnoreCase(permission)) {
            return;
        }
        throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidPermission());
    }

    public static String convertToExternalPermission(int mask) {
        String permission;
        switch (mask) {
        case 16:
            permission = ADMINISTRATION;
            break;
        case 32:
            permission = MANAGEMENT;
            break;
        case 64:
            permission = OPERATION;
            break;
        case 1:
            permission = READ;
            break;
        case 128:
            permission = DATA_QUERY;
            break;
        case 0:
            return EMPTY;
        default:
            throw new KylinException(PERMISSION_DENIED, "Invalid permission state: " + mask);
        }
        return permission;
    }

    // ============================================================================

    public abstract void init();

    /**
     * Checks if a user has permission on an entity.
     * 
     * @param user
     * @param userRoles
     * @param entityType String constants defined in AclEntityType 
     * @param entityUuid
     * @param permission
     * 
     * @return true if has permission
     */
    public abstract boolean checkPermission(String user, List<String> userRoles, //
            String entityType, String entityUuid, Permission permission);

    /**
     * Returns all granted permissions on specified entity.
     * 
     * @param entityType String constants defined in AclEntityType
     * @param entityUuid
     * @return a list of (user/role, permission)
     */
    public abstract List<Pair<String, AclPermission>> getAcl(String entityType, String entityUuid);

}
