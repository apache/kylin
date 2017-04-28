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

package org.apache.kylin.jdbc;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.kylin.jdbc.KylinMeta.KMetaCatalog;
import org.apache.kylin.jdbc.KylinMeta.KMetaColumn;
import org.apache.kylin.jdbc.KylinMeta.KMetaProject;
import org.apache.kylin.jdbc.KylinMeta.KMetaSchema;
import org.apache.kylin.jdbc.KylinMeta.KMetaTable;

/**
 */
public class DummyClient implements IRemoteClient {

    public DummyClient(KylinConnection conn) {
    }

    @Override
    public void connect() throws IOException {
    }

    @Override
    public KMetaProject retrieveMetaData(String project) throws IOException {
        List<KMetaColumn> columns = new ArrayList<KMetaColumn>();

        KMetaTable table = new KMetaTable("dummy", "dummy", "dummy", "dummy", columns);
        List<KMetaTable> tables = new ArrayList<KMetaTable>();
        tables.add(table);

        KMetaSchema schema = new KMetaSchema("dummy", "dummy", tables);
        List<KMetaSchema> schemas = new ArrayList<KMetaSchema>();
        schemas.add(schema);

        KMetaCatalog catalog = new KMetaCatalog("dummay", schemas);
        List<KMetaCatalog> catalogs = new ArrayList<KMetaCatalog>();
        catalogs.add(catalog);

        return new KMetaProject(project, catalogs);
    }

    @Override
    public QueryResult executeQuery(String sql, List<AvaticaParameter> params, List<Object> paramValues, Map<String, String> queryToggles) throws IOException {
        List<Object> data = new ArrayList<Object>();
        Object[] row = new Object[] { "foo", "bar", "tool" };
        data.add(row);

        List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));

        return new QueryResult(meta, data);
    }

    @Override
    public void close() throws IOException {
    }

}
