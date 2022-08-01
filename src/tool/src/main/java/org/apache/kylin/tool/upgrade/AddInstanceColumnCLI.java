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
package org.apache.kylin.tool.upgrade;

import java.util.Locale;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.util.Unsafe;
import org.springframework.jdbc.core.JdbcTemplate;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddInstanceColumnCLI {
    private static final String SHOW_COLUMNS_FROM_SQL = "SELECT column_name FROM information_schema.columns WHERE table_name='%s' and column_name='%s'";
    private static final String ADD_COL_TO_TABLE_SQL = "alter table %s add %s %s";
    private static final String INSTANCE = "instance";

    public static void main(String[] args) throws Exception {
        log.info("Start to add instance column to audit log...");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        try (val auditLogStore = new JdbcAuditLogStore(kylinConfig)) {
            String auditLogTable = auditLogStore.getTable();
            String checkSql = String.format(Locale.ROOT, SHOW_COLUMNS_FROM_SQL, auditLogTable, INSTANCE);
            String upgradeSql = String.format(Locale.ROOT, ADD_COL_TO_TABLE_SQL, auditLogTable, INSTANCE,
                    "varchar(100)");
            checkAndUpgrade(auditLogStore.getJdbcTemplate(), checkSql, upgradeSql);
        }
        log.info("Add instance column finished!");
        Unsafe.systemExit(0);
    }

    public static void checkAndUpgrade(JdbcTemplate jdbcTemplate, String checkSql, String upgradeSql) {
        val list = jdbcTemplate.queryForList(checkSql, String.class);
        if (CollectionUtils.isEmpty(list)) {
            log.info("Result of {} is empty, will execute {}", checkSql, upgradeSql);
            try {
                jdbcTemplate.execute(upgradeSql);
            } catch (Exception e) {
                log.error("Failed to execute upgradeSql: {}", upgradeSql, e);
            }
        } else {
            log.info("Result of {} is not empty, no need to upgrade.", checkSql);
        }
    }

}
