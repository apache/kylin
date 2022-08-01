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
package org.apache.kylin.common.persistence.metadata;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.AddressUtil;

import lombok.val;

public class JdbcAuditLogStoreTool {

    public static JdbcAuditLogStore prepareJdbcAuditLogStore(KylinConfig config) throws Exception {

        val url = config.getMetadataUrl();
        val auditLogStore = new JdbcAuditLogStore(config);

        val jdbcTemplate = auditLogStore.getJdbcTemplate();
        for (int i = 0; i < 100; i++) {
            val projectName = "p" + i;
            String unitId = RandomUtil.randomUUIDStr();
            jdbcTemplate.update(String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, "test_audit_log"),
                    "/" + projectName + "/aa", "aa".getBytes(StandardCharsets.UTF_8), System.currentTimeMillis(), 0,
                    unitId, null, AddressUtil.getLocalInstance());
        }

        return auditLogStore;
    }
}
