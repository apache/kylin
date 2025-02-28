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
package org.apache.kylin.rest.service;

import java.time.Clock;
import java.time.LocalDateTime;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.helper.MetadataToolHelper;
import org.apache.kylin.tool.HDFSMetadataTool;
import org.springframework.stereotype.Service;

import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

@Service
public class MetadataBackupService {

    private final MetadataToolHelper helper = new MetadataToolHelper();

    @SneakyThrows(Exception.class)
    public Pair<String, String> backupAll() {
        var backupPath = Pair.newPair("", "");
        try (SetThreadName ignored = new SetThreadName("MetadataBackupWorker")) {
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
            val backupConfig = kylinConfig.getMetadataBackupFromSystem() ? kylinConfig
                    : KylinConfig.createKylinConfig(kylinConfig);
            backupPath = helper.backup(backupConfig, null, getBackupDir(kylinConfig), null, true, false);
            helper.rotateAuditLog();
        }
        return backupPath;
    }

    public String backupProject(String project) throws Exception {
        val folder = LocalDateTime.now(Clock.systemDefaultZone()).format(MetadataToolHelper.DATE_TIME_FORMATTER)
                + "_backup";
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        val backupConfig = kylinConfig.getMetadataBackupFromSystem() ? kylinConfig
                : KylinConfig.createKylinConfig(kylinConfig);
        String backupDir = getBackupDir(kylinConfig);
        helper.backup(backupConfig, project, backupDir, folder, true, false);
        return StringUtils.appendIfMissing(backupDir, "/") + folder;
    }

    private String getBackupDir(KylinConfig kylinConfig) {
        return HadoopUtil.getBackupFolder(kylinConfig);

    }

}
