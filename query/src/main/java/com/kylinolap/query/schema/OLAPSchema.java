/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.query.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.AbstractSchema;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.project.ProjectInstance;
import com.kylinolap.cube.project.ProjectManager;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.schema.TableDesc;

/**
 * @author xjiang
 */
public class OLAPSchema extends AbstractSchema {

    // static {
    // addHookHandler();
    // }
    //
    // private static void addHookHandler() {
    // Function1<Object, Object> handler = new Function1<Object, Object>() {
    // public Object apply(Object a0) {
    // if (a0 != null) {
    // Object[] parseTree = (Object[]) a0;
    // OLAPRel._localContext.set(new OLAPContext());
    // OLAPRel._localContext.get().sql = (String) parseTree[0];
    // }
    // return a0;
    // }
    // };
    // Hook.PARSE_TREE.add(handler);
    // }

    private KylinConfig config;
    private String projectName;
    private String schemaName;
    private String storageUrl;
    private String starSchemaUrl;
    private String starSchemaUser;
    private String starSchemaPassword;

    private void init() {
        this.config = KylinConfig.getInstanceFromEnv();
        this.storageUrl = config.getStorageUrl();
        this.starSchemaUrl = config.getHiveUrl();
        this.starSchemaUser = config.getHiveUser();
        this.starSchemaPassword = config.getHivePassword();
    }

    public OLAPSchema(String project, String schemaName) {
        this.projectName = ProjectInstance.getNormalizedProjectName(project);
        this.schemaName = schemaName;
        init();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return buildTableMap();
    }

    private Map<String, Table> buildTableMap() {
        Map<String, Table> olapTables = new HashMap<String, Table>();
        List<TableDesc> projectTables = getProjectManager().listExposedTables(projectName);

        for (TableDesc tableDesc : projectTables) {
            final String tableName = tableDesc.getName();
            final OLAPTable table = new OLAPTable(this, tableDesc);
            olapTables.put(tableName, table);
        }

        return olapTables;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getStorageUrl() {
        return storageUrl;
    }

    public boolean hasStarSchemaUrl() {
        return starSchemaUrl != null && !starSchemaUrl.isEmpty();
    }

    public String getStarSchemaUrl() {
        return starSchemaUrl;
    }

    public String getStarSchemaUser() {
        return starSchemaUser;
    }

    public String getStarSchemaPassword() {
        return starSchemaPassword;
    }

    public MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public String getProjectName() {
        return this.projectName;
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(config);
    }

    public ProjectManager getProjectManager() {
        return ProjectManager.getInstance(config);
    }
}
