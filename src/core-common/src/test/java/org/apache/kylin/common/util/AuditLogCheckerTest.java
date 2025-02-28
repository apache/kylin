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

import static org.apache.kylin.common.util.AuditLogChecker.verify;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
class AuditLogCheckerTest {
    private static final String LOCAL_INSTANCE = "127.0.0.1";

    @Test
    public void testVerify() {
        byte[] removePatch = "[{\"op\":\"remove\",\"path\":\"\"}]".getBytes(StandardCharsets.UTF_8);
        AuditLog auditLog = new AuditLog(1L, "PROJECT/abc", ByteSource.wrap(removePatch), System.currentTimeMillis(),
                0L, UUID.randomUUID().toString(), null, null, LOCAL_INSTANCE,
                null, true);
        try {
            verify(auditLog, false);
            Assertions.fail("Not patch mode, but audit log is patch format!");
        } catch (Exception e) {
            // pass
        }

        verify(new AuditLog(1L, "PROJECT/abc",
                ByteSource.wrap("[{\"op\": \"value1\", \"path\": \"\"}]".getBytes(StandardCharsets.UTF_8)),
                System.currentTimeMillis(), 0L, UUID.randomUUID().toString(), null,
                null, LOCAL_INSTANCE, null, true), true);
    }
}
