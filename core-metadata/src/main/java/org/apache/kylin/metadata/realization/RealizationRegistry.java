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

package org.apache.kylin.metadata.realization;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class RealizationRegistry {

    private static final Logger logger = LoggerFactory.getLogger(RealizationRegistry.class);

    public static RealizationRegistry getInstance(KylinConfig config) {
        return config.getManager(RealizationRegistry.class);
    }

    // called by reflection
    static RealizationRegistry newInstance(KylinConfig config) throws IOException {
        return new RealizationRegistry(config);
    }

    // ============================================================================

    private Map<RealizationType, IRealizationProvider> providers;
    private KylinConfig config;

    private RealizationRegistry(KylinConfig config) throws IOException {
        logger.info("Initializing RealizationRegistry with metadata url " + config);
        this.config = config;
        init();
    }

    private void init() {
        providers = Maps.newConcurrentMap();

        // use reflection to load providers
        String[] providerNames = config.getRealizationProviders();
        for (String clsName : providerNames) {
            try {
                Class<? extends IRealizationProvider> cls = ClassUtil.forName(clsName, IRealizationProvider.class);
                IRealizationProvider p = (IRealizationProvider) cls.getMethod("getInstance", KylinConfig.class).invoke(null, config);
                providers.put(p.getRealizationType(), p);

            } catch (Exception | NoClassDefFoundError e) {
                if (e instanceof ClassNotFoundException || e instanceof NoClassDefFoundError)
                    logger.warn("Failed to create realization provider " + e);
                else
                    logger.error("Failed to create realization provider", e);
            }
        }

        if (providers.isEmpty())
            throw new IllegalArgumentException("Failed to find realization provider by url: " + config.getMetadataUrl());

        logger.info("RealizationRegistry is " + providers);
    }

    public Set<RealizationType> getRealizationTypes() {
        return Collections.unmodifiableSet(providers.keySet());
    }

    public IRealization getRealization(RealizationType type, String name) {
        IRealizationProvider p = providers.get(type);
        if (p == null) {
            logger.warn("No provider for realization type " + type);
            return null;
        }

        try {
            return p.getRealization(name);
        } catch (Exception ex) {
            // exception is possible if e.g. cube metadata is wrong
            logger.warn("Failed to load realization " + type + ":" + name, ex);
            return null;
        }
    }

}
