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

import java.io.IOException;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.constant.Constant;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class BroadcastSecurityContext {

    private static final ThreadLocal<Boolean> TRUSTED_BROADCAST = new ThreadLocal<>();

    private BroadcastSecurityContext() {
    }

    public static boolean isTrustedBroadcast() {
        return Boolean.TRUE.equals(TRUSTED_BROADCAST.get());
    }

    public static void runAsTrusted(CheckedRunnable action) throws IOException {
        boolean previous = isTrustedBroadcast();
        TRUSTED_BROADCAST.set(Boolean.TRUE);
        try {
            action.run();
        } finally {
            if (previous) {
                TRUSTED_BROADCAST.set(Boolean.TRUE);
            } else {
                TRUSTED_BROADCAST.remove();
            }
        }
    }

    public static void requireTrustedBroadcastOrGlobalAdmin() {
        if (isTrustedBroadcast()) {
            return;
        }

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        boolean globalAdmin = authentication != null && authentication.isAuthenticated()
                && authentication.getAuthorities().stream()
                        .anyMatch(authority -> Constant.ROLE_ADMIN.equals(authority.getAuthority()));
        if (!globalAdmin) {
            log.warn("Rejected an untrusted broadcast-originated ACL mutation");
            throw new KylinException(PERMISSION_DENIED, "Broadcast ACL mutation requires a trusted internal caller.");
        }
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws IOException;
    }
}
