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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.engine.data.TableSchema;
import org.apache.kylin.rest.constant.Constant;

public class SchemaMetaData {

    private final QueryExec queryExec;

    public SchemaMetaData(String project, KylinConfig kylinConfig) {
        queryExec = new QueryExec(project, kylinConfig);
    }

    public List<TableSchema> getTables() {
        return queryExec.getRootSchema().getSubSchemaMap().values().stream()
                .flatMap(schema -> getTables(schema).stream()).collect(Collectors.toList());
    }

    private List<TableSchema> getTables(CalciteSchema schema) {
        Map<String, Table> tables = new HashMap<>();
        String schemaName = schema.getName() == null ? Constant.FakeSchemaName : schema.getName();

        for (String tableName : schema.getTableNames()) {
            tables.put(tableName, schema.getTable(tableName, false).getTable());
        }
        tables.putAll(schema.getTablesBasedOnNullaryFunctions());

        List<TableSchema> tableSchemas = new LinkedList<>();
        tables.forEach((tableName, table) -> tableSchemas
                .add(convertToTableSchema(Constant.FakeCatalogName, schemaName, tableName, table)));
        return tableSchemas;
    }

    private TableSchema convertToTableSchema(String catalogName, String schemaName, String tableName, Table table) {
        return new TableSchema(catalogName, schemaName, tableName, table.getJdbcTableType().toString(), null,
                RelColumnMetaDataExtractor.getColumnMetadata(table.getRowType(javaTypeFactory())));
    }

    private JavaTypeFactory javaTypeFactory() {
        return TypeSystem.javaTypeFactory();
    }

}
