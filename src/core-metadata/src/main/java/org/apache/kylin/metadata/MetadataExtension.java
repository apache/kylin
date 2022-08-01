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

package org.apache.kylin.metadata;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.extension.ExtensionFactoryLoader;
import org.apache.kylin.metadata.query.QueryExcludedTablesExtension;

import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;

public class MetadataExtension {

    private static Factory extensionFactory = null;
    private static final ExtensionFactoryLoader<Factory> loader = new ExtensionFactoryLoader<>();

    private MetadataExtension() {
    }

    public static Factory getFactory() {
        if (extensionFactory == null) {
            synchronized (MetadataExtension.class) {
                extensionFactory = loader.loadFactory(Factory.class,
                        KylinConfig.getInstanceFromEnv().getMetadataExtensionFactory());
            }
        }
        return extensionFactory;
    }

    @VisibleForTesting
    public static void setFactory(Factory newFactory) {
        synchronized (MetadataExtension.class) {
            extensionFactory = newFactory;
        }
    }

    public static class Factory {

        public Factory() {
            // Do nothing
        }

        private QueryExcludedTablesExtension queryExcludedTablesExtension;

        public final QueryExcludedTablesExtension getQueryExcludedTablesExtension() {
            if (queryExcludedTablesExtension == null) {
                queryExcludedTablesExtension = createQueryExcludedTablesExtension();
            }
            return queryExcludedTablesExtension;
        }

        protected QueryExcludedTablesExtension createQueryExcludedTablesExtension() {
            return new QueryExcludedTablesExtensionDefault();
        }
    }

    private static class QueryExcludedTablesExtensionDefault implements QueryExcludedTablesExtension {
        @Override
        public Set<String> getExcludedTables(KylinConfig kylinConfig, String projectName) {
            return Sets.newHashSet();
        }

        @Override
        public void addExcludedTables(KylinConfig config, String projectName, String tableName, boolean isEnabled) {
            // do nothing
        }
    }
}
