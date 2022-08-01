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
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 */
public class NRealizationRegistry {

    private static final Logger logger = LoggerFactory.getLogger(NRealizationRegistry.class);

    public static NRealizationRegistry getInstance(KylinConfig config, String project) {
        return config.getManager(project, NRealizationRegistry.class);
    }

    // called by reflection
    static NRealizationRegistry newInstance(KylinConfig config, String project) throws IOException {
        return new NRealizationRegistry(config, project);
    }

    // ============================================================================

    private Map<String, IRealizationProvider> providers;
    private KylinConfig config;
    private String project;

    public NRealizationRegistry(KylinConfig config, String project) throws IOException {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NRealizationRegistry with KylinConfig Id: {} for project {}",
                    System.identityHashCode(config), project);
        this.config = config;
        this.project = project;
        init();
    }

    private void init() {
        providers = Maps.newConcurrentMap();

        // use reflection to load providers
        String[] providerNames = config.getRealizationProviders();
        for (String clsName : providerNames) {
            try {
                Class<? extends IRealizationProvider> cls = ClassUtil.forName(clsName, IRealizationProvider.class);
                IRealizationProvider p = (IRealizationProvider) cls
                        .getMethod("getInstance", KylinConfig.class, String.class).invoke(null, config, project);
                providers.put(p.getRealizationType(), p);

            } catch (Exception | NoClassDefFoundError e) {
                if (e instanceof ClassNotFoundException || e instanceof NoClassDefFoundError)
                    logger.warn("Failed to create realization provider " + e);
                else
                    logger.error("Failed to create realization provider", e);
            }
        }

        if (providers.isEmpty())
            throw new IllegalArgumentException("Failed to find realization provider");

        logger.info("RealizationRegistry is " + providers);
    }

    public Set<String> getRealizationTypes() {
        return Collections.unmodifiableSet(providers.keySet());
    }

    public IRealization getRealization(String realizationType, String name) {
        IRealizationProvider p = providers.get(realizationType);
        if (p == null) {
            logger.warn("No provider for realization type " + realizationType);
            return null;
        }

        try {
            return p.getRealization(name);
        } catch (Exception ex) {
            // exception is possible if e.g. cube metadata is wrong
            logger.warn("Failed to load realization " + realizationType + ":" + name, ex);
            return null;
        }
    }

}
