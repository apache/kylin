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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.FileSystemMetadataStore;
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
        validateMetadataStoreUrl(config, info);
        String metaDumpUrl = info.getDistMetaUrl();

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

    public static void validateMetadataStoreUrl(KylinConfig config, DumpInfo info) {
        validateMetadataStoreUrl(config, info.getProject(), info.getDistMetaUrl());
    }

    public static void validateMetadataStoreUrl(KylinConfig config, String project, String metaDumpUrl) {
        if (StringUtils.isBlank(metaDumpUrl)) {
            throw invalidMetadataStoreUrl();
        }

        StorageURL storageUrl;
        try {
            storageUrl = StorageURL.valueOf(metaDumpUrl);
        } catch (RuntimeException e) {
            throw invalidMetadataStoreUrl();
        }
        String scheme = storageUrl.getScheme();
        if (!FileSystemMetadataStore.HDFS_SCHEME.equalsIgnoreCase(scheme)
                && !FileSystemMetadataStore.FILE_SCHEME.equalsIgnoreCase(scheme)) {
            throw invalidMetadataStoreUrl();
        }
        if (!config.getMetadataUrlPrefix().equals(storageUrl.getIdentifier())) {
            throw invalidMetadataStoreUrl();
        }
        if (storageUrl.getAllParameters().size() != 1 || !storageUrl.containsParameter("path")
                || StringUtils.isBlank(storageUrl.getParameter("path"))) {
            throw invalidMetadataStoreUrl();
        }

        Path metadataStorePath = new Path(storageUrl.getParameter("path"));
        Path allowedRoot = new Path(new Path(config.getWorkingDirectoryWithConfiguredFs(project)), "job_tmp");
        if (!isStrictDescendant(metadataStorePath, allowedRoot)) {
            throw invalidMetadataStoreUrl();
        }
    }

    private static boolean isStrictDescendant(Path candidate, Path root) {
        try {
            URI candidateUri = normalize(candidate.toUri());
            URI rootUri = normalize(root.toUri());
            String candidateScheme = StringUtils.defaultIfBlank(candidateUri.getScheme(), rootUri.getScheme());
            String candidateAuthority = StringUtils.defaultIfBlank(candidateUri.getAuthority(), rootUri.getAuthority());
            if (!StringUtils.equalsIgnoreCase(candidateScheme, rootUri.getScheme())
                    || !StringUtils.equalsIgnoreCase(candidateAuthority, rootUri.getAuthority())) {
                return false;
            }

            String candidatePath = candidateUri.getPath();
            String rootPath = rootUri.getPath();
            if (StringUtils.isBlank(candidatePath) || StringUtils.isBlank(rootPath)) {
                return false;
            }
            if (!rootPath.endsWith("/")) {
                rootPath += "/";
            }
            return candidatePath.startsWith(rootPath);
        } catch (IllegalArgumentException | URISyntaxException e) {
            return false;
        }
    }

    private static URI normalize(URI uri) throws URISyntaxException {
        return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, null).normalize();
    }

    private static KylinException invalidMetadataStoreUrl() {
        return new KylinException(INVALID_PARAMETER, "Invalid metadata dump URL.");
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
