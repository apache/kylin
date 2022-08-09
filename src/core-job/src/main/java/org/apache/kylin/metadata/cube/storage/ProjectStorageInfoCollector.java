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

package org.apache.kylin.metadata.cube.storage;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ProjectStorageInfoCollector {

    private List<StorageInfoCollector> collectors = Lists.newArrayList();

    private static final ImmutableMap<Class, StorageInfoEnum> collectorType = ImmutableMap
            .<Class, StorageInfoEnum> builder().put(GarbageStorageCollector.class, StorageInfoEnum.GARBAGE_STORAGE)
            .put(TotalStorageCollector.class, StorageInfoEnum.TOTAL_STORAGE)
            .put(StorageQuotaCollector.class, StorageInfoEnum.STORAGE_QUOTA).build();

    public ProjectStorageInfoCollector(List<StorageInfoEnum> storageInfoList) {
        if (CollectionUtils.isNotEmpty(storageInfoList)) {
            storageInfoList.forEach(si -> addCollectors(si));
        }
    }

    private void collect(KylinConfig config, String project, StorageVolumeInfo storageVolumeInfo) {
        for (StorageInfoCollector collector : collectors) {
            try {
                collector.collect(config, project, storageVolumeInfo);
            } catch (Exception e) {
                storageVolumeInfo.getThrowableMap().put(collectorType.get(collector.getClass()), e);
            }
        }
    }

    private void addCollectors(StorageInfoEnum storageInfoEnum) {
        switch (storageInfoEnum) {
        case GARBAGE_STORAGE:
            collectors.add(new GarbageStorageCollector());
            break;
        case TOTAL_STORAGE:
            collectors.add(new TotalStorageCollector());
            break;
        case STORAGE_QUOTA:
            collectors.add(new StorageQuotaCollector());
            break;
        default:
            break;
        }
    }

    public StorageVolumeInfo getStorageVolumeInfo(KylinConfig config, String project) {
        StorageVolumeInfo storageVolumeInfo = new StorageVolumeInfo();
        if (StringUtils.isBlank(project) || CollectionUtils.isEmpty(collectors)) {
            return storageVolumeInfo;
        }
        collect(config, project, storageVolumeInfo);
        return storageVolumeInfo;
    }
}
