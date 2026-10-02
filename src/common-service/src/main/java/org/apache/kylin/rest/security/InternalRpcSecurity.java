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

import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.rest.constant.Constant;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

public final class InternalRpcSecurity {

    private InternalRpcSecurity() {
    }

    public static boolean isGlobalAdmin() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return authentication != null && authentication.isAuthenticated()
                && authentication.getAuthorities().stream()
                        .anyMatch(authority -> Constant.ROLE_ADMIN.equals(authority.getAuthority()));
    }

    public static boolean hasValidServiceToken(HttpServletRequest request) {
        String configuredToken = KylinConfig.getInstanceFromEnv().getBroadcastToken();
        String providedToken = request == null ? null
                : request.getHeader(BroadcastEventReadyNotifier.BROADCAST_TOKEN_HEADER);
        return StringUtils.isNotBlank(configuredToken) && providedToken != null
                && MessageDigest.isEqual(configuredToken.getBytes(StandardCharsets.UTF_8),
                        providedToken.getBytes(StandardCharsets.UTF_8));
    }

    public static void requireGlobalAdminOrServiceToken(HttpServletRequest request) {
        if (!isGlobalAdmin() && !hasValidServiceToken(request)) {
            throw new KylinException(PERMISSION_DENIED,
                    "Internal RPC requires an authenticated global admin or a valid service token.");
        }
    }
}
