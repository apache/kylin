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

package org.apache.kylin.tool.garbage;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.metadata.sourceusage.SourceUsageRecord;

public class SourceUsageCleaner {

    public void cleanup() {

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        long expirationTime = config.getSourceUsageSurvivalTimeThreshold();

        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(config);
        List<SourceUsageRecord> allRecords = sourceUsageManager.getAllRecordsWithoutInit();
        int totalSize = allRecords.size();
        if (totalSize <= 1)
            return;
        List<SourceUsageRecord> collect = allRecords.stream().filter(
                sourceUsageRecord -> (System.currentTimeMillis() - sourceUsageRecord.getCreateTime()) >= expirationTime)
                .sorted(Comparator.comparingLong(SourceUsageRecord::getCreateTime)).collect(Collectors.toList());
        if (collect.size() == totalSize) {
            collect.remove(totalSize - 1);
        }
        for (SourceUsageRecord record : collect) {
            sourceUsageManager.delSourceUsage(record.resourceName());
        }
    }
}
