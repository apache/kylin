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

package org.apache.kylin.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.extension.ExtensionFactoryLoader;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.engine.SchemaMapExtension;
import org.apache.kylin.query.engine.TableColumnAuthExtension;

public class QueryExtension {

    private static final ExtensionFactoryLoader<Factory> loader = new ExtensionFactoryLoader<>();
    private static Factory extensionFactory = null;

    private QueryExtension() {
    }

    public static Factory getFactory() {
        if (extensionFactory == null) {
            synchronized (QueryExtension.class) {
                extensionFactory = loader.loadFactory(Factory.class,
                        KylinConfig.getInstanceFromEnv().getQueryExtensionFactory());
            }
        }
        return extensionFactory;
    }

    @VisibleForTesting
    public static void setFactory(Factory newFactory) {
        synchronized (QueryExtension.class) {
            extensionFactory = newFactory;
        }
    }

    public static class Factory {

        private SchemaMapExtension schemaMapExtension;
        private TableColumnAuthExtension tableColumnAuthExtension;

        public Factory() {
            // Do nothing
        }

        public SchemaMapExtension getSchemaMapExtension() {
            if (schemaMapExtension == null) {
                schemaMapExtension = createSchemaMapExtension();
            }
            return schemaMapExtension;
        }

        public TableColumnAuthExtension getTableColumnAuthExtension() {
            if (tableColumnAuthExtension == null) {
                tableColumnAuthExtension = createTableColumnAuthExtension();
            }
            return tableColumnAuthExtension;
        }

        // ------------------------- private method

        protected SchemaMapExtension createSchemaMapExtension() {
            return new SchemaMapExtensionDefault();
        }

        protected TableColumnAuthExtension createTableColumnAuthExtension() {
            return new TableColumnAuthExtensionDefault();
        }
    }

    // ========================= Default Impl of QueryExtension

    private static class SchemaMapExtensionDefault implements SchemaMapExtension {
        @Override
        public Map<String, List<TableDesc>> getAuthorizedTablesAndColumns(KylinConfig kylinConfig, String projectName,
                boolean fullyAuthorized, String userName, Set<String> groups) {
            return NTableMetadataManager.getInstance(kylinConfig, projectName).listTablesGroupBySchema();
        }
    }

    private static class TableColumnAuthExtensionDefault implements TableColumnAuthExtension {
        @Override
        public boolean isColumnsAuthorized(KylinConfig kylinConfig, String projectName, String user, Set<String> groups,
                Set<String> columns) {
            return true;
        }
    }
}
