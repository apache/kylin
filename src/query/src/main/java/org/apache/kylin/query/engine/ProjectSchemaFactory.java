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

package org.apache.kylin.query.engine;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.DatabaseDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.QueryExtension;
import org.apache.kylin.query.engine.view.ViewAnalyzer;
import org.apache.kylin.query.engine.view.ViewSchema;
import org.apache.kylin.query.schema.OlapSchema;
import org.apache.kylin.rest.constant.Constant;

import lombok.extern.slf4j.Slf4j;

/**
 * factory that create and construct schemas within a project
 */
@Slf4j
class ProjectSchemaFactory {

    private final String projectName;
    private final KylinConfig kylinConfig;
    private final Map<String, List<TableDesc>> schemasMap;
    private final Map<String, List<NDataModel>> modelsMap;
    private final String defaultSchemaName;

    ProjectSchemaFactory(String projectName, KylinConfig kylinConfig) {
        this.projectName = projectName;
        this.kylinConfig = kylinConfig;

        NProjectManager npr = NProjectManager.getInstance(kylinConfig);
        QueryContext.AclInfo aclInfo = QueryContext.current().getAclInfo();
        String user = Objects.nonNull(aclInfo) ? aclInfo.getUsername() : null;
        Set<String> groups = Objects.nonNull(aclInfo) ? aclInfo.getGroups() : null;
        schemasMap = QueryExtension.getFactory().getSchemaMapExtension().getAuthorizedTablesAndColumns(kylinConfig,
                projectName, aclDisabledOrIsAdmin(aclInfo), user, groups);
        removeStreamingTables(schemasMap, kylinConfig.isStreamingEnabled());
        modelsMap = NDataflowManager.getInstance(kylinConfig, projectName).getModelsGroupbyTable();

        // "database" in TableDesc correspond to our schema
        // the logic to decide which schema to be "default" in calcite:
        // if some schema are named "default", use it.
        // other wise use the schema with most tables
        String majoritySchemaName = npr.getDefaultDatabase(projectName);
        // UT
        if (Objects.isNull(majoritySchemaName)) {
            majoritySchemaName = DatabaseDesc.getDefaultDatabaseByMaxTables(schemasMap);
        }
        defaultSchemaName = majoritySchemaName;
    }

    /**
     * remove streaming tables when streaming function is disabled
     */
    private void removeStreamingTables(Map<String, List<TableDesc>> schemasMap, boolean streamingEnabled) {
        schemasMap.values()
                .forEach(tableDescList -> tableDescList.removeIf(table -> !table.isAccessible(streamingEnabled)));
        schemasMap.keySet().removeIf(key -> CollectionUtils.isEmpty(schemasMap.get(key)));
    }

    private boolean aclDisabledOrIsAdmin(QueryContext.AclInfo aclInfo) {
        return !kylinConfig.isAclTCREnabled()
                || Objects.nonNull(aclInfo) && (CollectionUtils.isNotEmpty(aclInfo.getGroups())
                        && aclInfo.getGroups().stream().anyMatch(Constant.ROLE_ADMIN::equals))
                || Objects.nonNull(aclInfo) && aclInfo.isHasAdminPermission();
    }

    public CalciteSchema createProjectRootSchema() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        addProjectSchemas(rootSchema);

        return rootSchema;
    }

    public String getDefaultSchema() {
        return defaultSchemaName;
    }

    private void addProjectSchemas(CalciteSchema parentSchema) {
        addUDFs(parentSchema);
        for (String schemaName : schemasMap.keySet()) {
            parentSchema.add(schemaName, createSchema(schemaName));
        }
    }

    public void addModelViewSchemas(CalciteSchema parentSchema, ViewAnalyzer viewAnalyzer) {
        final String viewSchemaName = projectName;

        ViewSchema viewSchema = new ViewSchema(viewSchemaName, viewAnalyzer);
        CalciteSchema schema = getOrCreateViewSchema(parentSchema, viewSchema);
        SchemaPlus plus = schema.plus();

        List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, projectName).listOnlineDataModels();
        for (NDataModel model : models) {
            if (schema.getTable(model.getAlias(), false) == null) {
                viewSchema.addModel(plus, model);
            } else {
                log.warn("Auto model view creation for {}.{} failed, name collision with source tables", viewSchemaName,
                        viewSchemaName);
            }
        }
    }

    private CalciteSchema getOrCreateViewSchema(CalciteSchema parentSchema, ViewSchema viewSchema) {
        String schemaName = viewSchema.getSchemaName();
        CalciteSchema existingSchema = parentSchema.getSubSchema(schemaName, false);
        if (existingSchema != null) {
            return existingSchema;
        }

        CalciteSchema addedViewSchema = parentSchema.add(schemaName, viewSchema);
        addUDFs(addedViewSchema);
        return addedViewSchema;
    }

    private Schema createSchema(String schemaName) {
        return new OlapSchema(kylinConfig, projectName, schemaName, schemasMap.get(schemaName), modelsMap);
    }

    private void addUDFs(CalciteSchema calciteSchema) {
        for (Map.Entry<String, Function> entry : UdfRegistry.allUdfMap.entries()) {
            calciteSchema.plus().add(entry.getKey().toUpperCase(Locale.ROOT), entry.getValue());
        }
    }
}
