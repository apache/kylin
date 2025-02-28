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

package org.apache.kylin.util;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataDumpUtil {

    private static final String EMPTY = "";

    public static void dumpMetadata(DumpInfo info) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String metaDumpUrl = info.getDistMetaUrl();

        if (StringUtils.isEmpty(metaDumpUrl)) {
            throw new RuntimeException("Missing metaUrl");
        }

        final Properties props = config.exportToProperties();
        props.setProperty("kylin.metadata.url", metaDumpUrl);

        KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
        MetadataStore dstMetadataStore = MetadataStore.createMetadataStore(dstConfig);

        if (info.getType() == DumpInfo.DumpType.DATA_LOADING) {
            dumpMetadataViaTmpDir(config, dstMetadataStore, info);
        } else if (info.getType() == DumpInfo.DumpType.ASYNC_QUERY) {
            dstMetadataStore.dump(ResourceStore.getKylinMetaStore(config), info.getMetadataDumpList());
        }
        log.debug("Dump metadata finished.");
    }

    private static void dumpMetadataViaTmpDir(KylinConfig config, MetadataStore dstMetadataStore, DumpInfo info)
            throws IOException {
        File tmpDir = File.createTempFile("kylin_job_meta", EMPTY);
        FileUtils.forceDelete(tmpDir); // we need a directory, so delete the file first

        // The way of Updating metadata is CopyOnWrite. So it is safe to use Reference in the value.
        Map<String, RawResource> dumpMap = EnhancedUnitOfWork
                .doInTransactionWithCheckAndRetry(UnitOfWorkParams.<Map<String, RawResource>> builder().readonly(true)
                        .unitName(info.getProject()).maxRetry(1).processor(() -> {
                            Map<String, RawResource> retMap = Maps.newHashMap();
                            for (String resPath : info.getMetadataDumpList()) {
                                ResourceStore resourceStore = ResourceStore.getKylinMetaStore(config);
                                RawResource rawResource = resourceStore.getResource(resPath);
                                retMap.put(resPath, rawResource);
                            }
                            return retMap;
                        }).build());

        if (Objects.isNull(dumpMap) || dumpMap.isEmpty()) {
            return;
        }
        // dump metadata
        ResourceStore.dumpResourceMaps(tmpDir, dumpMap);
        // copy metadata to target metaUrl
        dstMetadataStore.uploadFromFile(tmpDir);
        // clean up
        log.debug("Copied metadata to the target metaUrl, delete the temp dir: {}", tmpDir);
        FileUtils.forceDelete(tmpDir);
    }

}
