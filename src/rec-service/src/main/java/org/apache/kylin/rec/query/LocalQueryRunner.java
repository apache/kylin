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

package org.apache.kylin.rec.query;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.query.util.EscapeTransformer;
import org.apache.kylin.rec.query.mockup.MockupPushDownRunner;
import org.apache.kylin.rec.util.OptimizeTransformer;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class LocalQueryRunner extends AbstractQueryRunner {

    private final Set<String> dumpResources;
    private final Map<String, RootPersistentEntity> mockupResources;
    private static final String OPTIMIZE_TRANSFORMER = OptimizeTransformer.class.getName();
    private static final String ESCAPE_TRANSFORMER = EscapeTransformer.class.getName();

    LocalQueryRunner(KylinConfig srcKylinConfig, String projectName, String[] sqls, Set<String> dumpResources,
            Map<String, RootPersistentEntity> mockupResources) {
        super(projectName, sqls);
        this.kylinConfig = srcKylinConfig;
        this.dumpResources = dumpResources;
        this.mockupResources = mockupResources;
    }

    @Override
    public KylinConfig prepareConfig() throws IOException {
        File tmp = File.createTempFile("kylin_job_meta", "");
        FileUtils.forceDelete(tmp);
        val properties = kylinConfig.exportToProperties();
        properties.setProperty("kylin.metadata.url", tmp.getAbsolutePath());
        ResourceStore.dumpResources(kylinConfig, tmp, dumpResources, properties);

        for (Map.Entry<String, RootPersistentEntity> mockupResource : mockupResources.entrySet()) {
            File dumpFile = new File(tmp, mockupResource.getKey() + ".json");
            File dumpParent = dumpFile.getParentFile();
            if (dumpParent.isFile()) {
                FileUtils.forceDelete(dumpParent);
            }
            FileUtils.forceMkdir(dumpParent);
            String dumpJson = JsonUtil.writeValueAsIndentString(mockupResource.getValue());
            FileUtils.writeStringToFile(dumpFile, dumpJson, Charset.defaultCharset());
        }

        KylinConfig config = KylinConfig.createKylinConfig(properties);
        List<String> transformers = Arrays.stream(kylinConfig.getQueryTransformers()).collect(Collectors.toList());
        int escapeIndex = transformers.indexOf(ESCAPE_TRANSFORMER);
        transformers.add(escapeIndex + 1, OPTIMIZE_TRANSFORMER);
        config.setProperty("kylin.query.pushdown.runner-class-name", MockupPushDownRunner.class.getName());
        config.setProperty("kylin.query.pushdown-enabled", "true");
        config.setProperty("kylin.query.transformers", StringUtils.join(transformers, ','));
        config.setProperty("kylin.query.security.acl-tcr-enabled", "false");
        config.setProperty("kylin.smart.conf.skip-corr-reduce-rule", "true");

        return config;
    }

    @Override
    public void cleanupConfig(KylinConfig config) throws IOException {
        File metaDir = new File(config.getMetadataUrl().getIdentifier());
        if (metaDir.exists() && metaDir.isDirectory()) {
            FileUtils.forceDelete(metaDir);
            log.debug("Deleted the meta dir: {}", metaDir);
        }

        if (MapUtils.isNotEmpty(mockupResources)) {
            mockupResources.clear();
        }
        if (CollectionUtils.isNotEmpty(dumpResources)) {
            dumpResources.clear();
        }

        config.clearManagers();
        config.clearManagersByProject(project);
        ResourceStore.clearCache(config);
    }
}
