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
package org.apache.kylin.common.util;

import org.apache.kylin.common.persistence.AuditLog;

import lombok.extern.slf4j.Slf4j;

/**
 * 10/01/2024 hellozepp(lisheng.zhanglin@163.com)
 */
@Slf4j
public class AuditLogChecker {
    private AuditLogChecker() {
    }

    public static void verify(final AuditLog auditLogs, boolean isPatchMode) {
        if (auditLogs == null) {
            return;
        }

        if (auditLogs.isDiffFlag() && !isPatchMode) {
            throw new IllegalArgumentException(
                    "audit log json patch is disabled, but AuditLog data is patch format! Please enable "
                            + "`kylin.metadata.audit-log-json-patch-enabled` or delete the patch data!");
        }
    }

}
