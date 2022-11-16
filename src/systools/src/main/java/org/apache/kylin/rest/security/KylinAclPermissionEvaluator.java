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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.rest.service.AclService;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.core.Authentication;

public class KylinAclPermissionEvaluator extends AclPermissionEvaluator {

    private PermissionFactory permissionFactory;

    public KylinAclPermissionEvaluator(AclService aclService, PermissionFactory permissionFactory) {
        super(aclService);
        super.setPermissionFactory(permissionFactory);
        this.permissionFactory = permissionFactory;
    }

    @Override
    public boolean hasPermission(Authentication authentication, Object targetDomainObject, Object permission) {
        if (Objects.isNull(targetDomainObject)) {
            return false;
        }
        //because Transaction(project= ) need project name, transfer project name to prjInstance here
        if (targetDomainObject instanceof String) {
            targetDomainObject = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .getProject(targetDomainObject.toString());
        }

        ExternalAclProvider eap = ExternalAclProvider.getInstance();
        if (Objects.isNull(eap)) {
            return super.hasPermission(authentication, targetDomainObject, permission);
        }

        AclEntity e = (AclEntity) targetDomainObject;
        return checkExternalPermission(eap, authentication, e.getClass().getSimpleName(), e.getId(), permission);
    }

    private boolean checkExternalPermission(ExternalAclProvider eap, Authentication authentication, String entityType,
            String entityUuid, Object permission) {

        String currentUser = authentication.getName();
        List<String> authorities = AclPermissionUtil.transformAuthorities(authentication.getAuthorities());
        List<Permission> permissions = resolveKylinPermission(permission);

        for (Permission p : permissions) {
            if (eap.checkPermission(currentUser, authorities, entityType, entityUuid, p)) {
                return true;
            }
        }
        return false;
    }

    private List<Permission> resolveKylinPermission(Object permission) {
        if (permission instanceof Integer) {
            return Arrays.asList(permissionFactory.buildFromMask(((Integer) permission).intValue()));
        }

        if (permission instanceof Permission) {
            return Arrays.asList((Permission) permission);
        }

        if (permission instanceof Permission[]) {
            return Arrays.asList((Permission[]) permission);
        }

        if (permission instanceof String) {
            String permString = (String) permission;
            Permission p;

            try {
                p = permissionFactory.buildFromName(permString);
            } catch (IllegalArgumentException notfound) {
                p = permissionFactory.buildFromName(permString.toUpperCase(Locale.ROOT));
            }

            if (Objects.nonNull(p)) {
                return Collections.singletonList(p);
            }

        }
        throw new IllegalArgumentException("Unsupported permission: " + permission);
    }

    @Override
    public boolean hasPermission(Authentication authentication, Serializable targetId, String targetType,
            Object permission) {
        ExternalAclProvider eap = ExternalAclProvider.getInstance();
        if (Objects.isNull(eap)) {
            return super.hasPermission(authentication, targetId, targetType, permission);
        }

        return checkExternalPermission(eap, authentication, targetType, targetId.toString(), permission);
    }
}
