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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_JDBC_SOURCE_CONFIG;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.model.ISourceAware;

import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.cache.RemovalListener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("UnstableApiUsage")
public class SourceFactory {
    private SourceFactory() {
    }

    private static final Cache<String, ISource> sourceMap;

    static {
        sourceMap = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.DAYS)
                .removalListener((RemovalListener<String, ISource>) entry -> {
                    ISource s = entry.getValue();
                    if (s != null) {
                        try {
                            s.close();
                        } catch (IOException e) {
                            log.error("Failed to close ISource: {}", s.getClass().getName(), e);
                        }
                    }
                }).build();
    }

    public static ISource getSparkSource() {
        return getSource(ISourceAware.ID_SPARK);
    }

    public static ISource getSource(ISourceAware aware) {
        String key = createSourceCacheKey(aware);
        synchronized (SourceFactory.class) {
            ISource source = sourceMap.getIfPresent(key);
            if (source != null) {
                return source;
            }
            source = createSource(aware);
            sourceMap.put(key, source);
            return source;
        }
    }

    public static ISource getSource(int sourceType) {
        return getSource(sourceType, KylinConfig.getInstanceFromEnv());
    }

    public static ISource getSource(int sourceType, KylinConfig kylinConfig) {
        return getSource(new ISourceAware() {
            @Override
            public int getSourceType() {
                return sourceType;
            }

            @Override
            public KylinConfig getConfig() {
                return kylinConfig;
            }
        });
    }

    private static ISource createSource(ISourceAware aware) {
        Map<Integer, String> sources = KylinConfig.getInstanceFromEnv().getSourceEngines();
        String clazz = sources.get(aware.getSourceType());
        try {
            return ClassUtil.forName(clazz, ISource.class).getDeclaredConstructor(KylinConfig.class)
                    .newInstance(aware.getConfig());
        } catch (Exception e) {
            log.error("Failed to create source: SourceType={}", aware.getSourceType());
            throw new KylinException(INVALID_JDBC_SOURCE_CONFIG, MsgPicker.getMsg().getJdbcConnectionInfoWrong(), e);
        }
    }

    private static String createSourceCacheKey(ISourceAware aware) {
        StringBuilder builder = new StringBuilder();
        builder.append(aware.getSourceType()).append('|');

        KylinConfig config = aware.getConfig();
        builder.append(config.getJdbcConnectionUrl()).append('|');
        builder.append(config.getJdbcUser()).append('|');
        builder.append(config.getJdbcPass()).append('|'); // In case password is wrong at the first time
        builder.append(config.getJdbcDriver()).append('|');
        builder.append(config.getJdbcDialect()).append('|');
        builder.append(config.getJdbcAdaptorClass()).append('|');
        return builder.toString();
    }

    public static <T> T createEngineAdapter(ISourceAware table, Class<T> engineInterface) {
        return getSource(table).adaptToBuildEngine(engineInterface);
    }

}
