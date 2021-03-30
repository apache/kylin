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

package org.apache.kylin.query.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;

/**
 */
public class OLAPSchema extends AbstractSchema {

    //    private static final Logger logger = LoggerFactory.getLogger(OLAPSchema.class);

    private KylinConfig config;
    private String projectName;
    private String schemaName;
    private boolean exposeMore;
    private StorageURL storageUrl;
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

    public OLAPSchema(String project, String schemaName, boolean exposeMore) {
        this.projectName = project;
        this.schemaName = schemaName;
        this.exposeMore = exposeMore;
        init();
    }

    /**
     * It is intended to skip caching, because underlying project/tables might change.
     *
     * @return
     */
    @Override
    public Map<String, Table> getTableMap() {
        return buildTableMap();
    }

    private Map<String, Table> buildTableMap() {
        Map<String, Table> olapTables = new HashMap<String, Table>();

        Collection<TableDesc> projectTables = ProjectManager.getInstance(config).listExposedTables(projectName,
                exposeMore);

        for (TableDesc tableDesc : projectTables) {
            if (tableDesc.getDatabase().equals(schemaName)) {
                final String tableName = tableDesc.getName();//safe to use tableDesc.getName() here, it is in a DB context now
                final OLAPTable table = new OLAPTable(this, tableDesc, exposeMore);
                olapTables.put(tableName, table);
                //logger.debug("Project " + projectName + " exposes table " + tableName);
            }
        }

        return olapTables;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public StorageURL getStorageUrl() {
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

    public DataModelManager getMetadataManager() {
        return DataModelManager.getInstance(config);
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

    public ProjectInstance getProjectInstance() {
        return ProjectManager.getInstance(config).getProject(projectName);
    }
}
