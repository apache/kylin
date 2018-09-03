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
package org.apache.kylin.source;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class SourceManager {
    private static final Logger logger = LoggerFactory.getLogger(SourceManager.class);

    private final KylinConfig systemConfig;
    private final Cache<String, ISource> sourceMap;

    public static SourceManager getInstance(KylinConfig config) {
        return config.getManager(SourceManager.class);
    }

    // called by reflection
    static SourceManager newInstance(KylinConfig config) throws IOException {
        return new SourceManager(config);
    }

    // ============================================

    private SourceManager(KylinConfig config) {
        this.systemConfig = config;
        this.sourceMap = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.DAYS)
                .removalListener(new RemovalListener<String, ISource>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, ISource> entry) {
                        ISource s = entry.getValue();
                        if (s != null) {
                            try {
                                s.close();
                            } catch (Throwable e) {
                                logger.error("Failed to close ISource: {}", s.getClass().getName(), e);
                            }
                        }
                    }
                }).build();
    }

    public ISource getCachedSource(ISourceAware aware) {
        String key = createSourceCacheKey(aware);
        ISource source = sourceMap.getIfPresent(key);
        if (source != null)
            return source;

        synchronized (this) {
            source = sourceMap.getIfPresent(key);
            if (source != null)
                return source;

            source = createSource(aware);
            sourceMap.put(key, source);
            return source;
        }
    }

    public ISource getProjectSource(String projectName) {
        ProjectInstance projectInstance = ProjectManager.getInstance(systemConfig).getProject(projectName);
        if (projectInstance != null)
            return getCachedSource(projectInstance);
        else
            return getDefaultSource();
    }

    private String createSourceCacheKey(ISourceAware aware) {
        StringBuilder builder = new StringBuilder();
        builder.append(aware.getSourceType()).append('|');

        KylinConfig config = aware.getConfig();
        builder.append(config.getJdbcSourceConnectionUrl()).append('|');
        builder.append(config.getJdbcSourceDriver()).append('|');
        builder.append(config.getJdbcSourceUser()).append('|');
        builder.append(config.getJdbcSourceFieldDelimiter()).append('|');
        builder.append(config.getJdbcSourceDialect()).append('|');
        return builder.toString(); // jdbc password not needed, because url+user should be identical.
    }

    private ISource createSource(ISourceAware aware) {
        String sourceClazz = systemConfig.getSourceEngines().get(aware.getSourceType());
        try {
            return ClassUtil.forName(sourceClazz, ISource.class).getDeclaredConstructor(KylinConfig.class)
                    .newInstance(aware.getConfig());
        } catch (Throwable e) {
            logger.error("Failed to create source: SourceType={}", aware.getSourceType(), e);
            return null;
        }
    }

    // ==========================================================

    public static ISource getSource(ISourceAware aware) {
        return getInstance(aware.getConfig()).getCachedSource(aware);
    }

    public static ISource getDefaultSource() {
        final KylinConfig sysConfig = KylinConfig.getInstanceFromEnv();
        return getSource(new ISourceAware() {
            @Override
            public int getSourceType() {
                return sysConfig.getDefaultSource();
            }

            @Override
            public KylinConfig getConfig() {
                return sysConfig;
            }
        });
    }

    public static IReadableTable createReadableTable(TableDesc table, String uuid) {
        return getSource(table).createReadableTable(table, uuid);
    }

    public static <T> T createEngineAdapter(ISourceAware table, Class<T> engineInterface) {
        return getSource(table).adaptToBuildEngine(engineInterface);
    }

    public static List<String> getMRDependentResources(TableDesc table) {
        return getSource(table).getSourceMetadataExplorer().getRelatedKylinResources(table);
    }
}
