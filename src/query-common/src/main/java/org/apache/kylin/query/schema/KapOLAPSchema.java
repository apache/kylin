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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.NDataModel;

public class KapOLAPSchema extends OLAPSchema {
    private String schemaName;
    private List<TableDesc> tables;
    private Map<String, List<NDataModel>> modelsMap;

    public KapOLAPSchema(String project, String schemaName, List<TableDesc> tables,
            Map<String, List<NDataModel>> modelsMap) {
        super(project, schemaName, tables);
        this.schemaName = schemaName;
        this.tables = tables;
        this.modelsMap = modelsMap;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return createTableMap();
    }

    public boolean hasTables() {
        return tables != null && !tables.isEmpty();
    }

    private Map<String, Table> createTableMap() {
        Map<String, Table> olapTables = new HashMap<>();

        for (TableDesc tableDesc : tables) {
            final String tableName = tableDesc.getName();//safe to use tableDesc.getUuid() here, it is in a DB context now
            final KapOLAPTable table = new KapOLAPTable(this, tableDesc, modelsMap);
            olapTables.put(tableName, table);
        }

        return olapTables;
    }
}
