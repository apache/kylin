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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.constants.AclConstants;
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

    // used by ranger ExternalAclProvider
    public static String convertToExternalPermission(Permission p) {
        String permString;
        if (BasePermission.ADMINISTRATION.equals(p)) {
            permString = AclConstants.ADMINISTRATION;
        } else if (AclPermission.MANAGEMENT.equals(p)) {
            permString = AclConstants.MANAGEMENT;
        } else if (AclPermission.OPERATION.equals(p)) {
            permString = AclConstants.OPERATION;
        } else if (BasePermission.READ.equals(p)) {
            permString = AclConstants.READ;
        } else if (AclPermission.DATA_QUERY.equals(p)) {
            permString = AclConstants.DATA_QUERY;
        } else {
            permString = p.getPattern();
        }
        return permString;
    }

    public static void checkExternalPermission(String permission) {
        if (StringUtils.isBlank(permission)) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getEmptyPermission());
        }
        if (AclConstants.ADMINISTRATION.equalsIgnoreCase(permission)
                || AclConstants.MANAGEMENT.equalsIgnoreCase(permission)
                || AclConstants.OPERATION.equalsIgnoreCase(permission)
                || AclConstants.READ.equalsIgnoreCase(permission)) {
            return;
        }
        throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidPermission());
    }

    public static String convertToExternalPermission(int mask) {
        switch (mask) {
        case 16:
            return AclConstants.ADMINISTRATION;
        case 32:
            return AclConstants.MANAGEMENT;
        case 64:
            return AclConstants.OPERATION;
        case 1:
            return AclConstants.READ;
        case 128:
            return AclConstants.DATA_QUERY;
        case 0:
            return AclConstants.EMPTY;
        default:
            throw new KylinException(PERMISSION_DENIED, "Invalid permission state: " + mask);
        }
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
