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

import org.apache.kylin.jdbc.KylinMeta.KMetaCatalog;
import org.apache.kylin.jdbc.KylinMeta.KMetaColumn;
import org.apache.kylin.jdbc.KylinMeta.KMetaProject;
import org.apache.kylin.jdbc.KylinMeta.KMetaSchema;
import org.apache.kylin.jdbc.KylinMeta.KMetaTable;
import org.apache.kylin.jdbc.json.GenericResponse;
import org.apache.kylin.jdbc.json.SQLResponseStub;
import org.apache.kylin.jdbc.json.StatementParameter;

/**
 */
public class DummyClient extends KylinClient {

    public DummyClient(KylinConnection conn) {
        super(conn);
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

        KMetaCatalog catalog = new KMetaCatalog("dummy", schemas);
        List<KMetaCatalog> catalogs = new ArrayList<KMetaCatalog>();
        catalogs.add(catalog);

        return new KMetaProject(project, catalogs);
    }

    @Override
    public GenericResponse<SQLResponseStub> executeKylinQuery(String sql, List<StatementParameter> params,
                                             Map<String, String> queryToggles, String queryId) throws IOException {
        SQLResponseStub sqlResponseStub = new SQLResponseStub();

        List<SQLResponseStub.ColumnMetaStub> meta = new ArrayList<>();
        SQLResponseStub.ColumnMetaStub column1 = new SQLResponseStub.ColumnMetaStub();
        column1.setColumnType(Types.VARCHAR);
        column1.setColumnTypeName("varchar");
        column1.setIsNullable(1);
        meta.add(column1);

        SQLResponseStub.ColumnMetaStub column2 = new SQLResponseStub.ColumnMetaStub();
        column2.setColumnType(Types.VARCHAR);
        column2.setColumnTypeName("varchar");
        column2.setIsNullable(1);
        meta.add(column2);

        SQLResponseStub.ColumnMetaStub column3 = new SQLResponseStub.ColumnMetaStub();
        column3.setColumnType(Types.VARCHAR);
        column3.setColumnTypeName("varchar");
        column3.setIsNullable(1);
        meta.add(column3);

        SQLResponseStub.ColumnMetaStub column4 = new SQLResponseStub.ColumnMetaStub();
        column4.setColumnType(Types.DATE);
        column4.setColumnTypeName("date");
        column4.setIsNullable(1);
        meta.add(column4);

        SQLResponseStub.ColumnMetaStub column5 = new SQLResponseStub.ColumnMetaStub();
        column5.setColumnType(Types.TIME);
        column5.setColumnTypeName("time");
        column5.setIsNullable(1);
        meta.add(column5);

        SQLResponseStub.ColumnMetaStub column6 = new SQLResponseStub.ColumnMetaStub();
        column6.setColumnType(Types.TIMESTAMP);
        column6.setColumnTypeName("timestamp");
        column6.setIsNullable(1);
        meta.add(column6);

        sqlResponseStub.setColumnMetas(meta);

        List<String[]> data = new ArrayList<String[]>();

        String[] row = new String[] { "foo", "bar", "tool", "2019-04-27", "17:30:03", "2019-04-27 17:30:03.123" };
        data.add(row);

        sqlResponseStub.setResults(data);

        sqlResponseStub.setQueryId(queryId);

        GenericResponse<SQLResponseStub> rslt = new GenericResponse<>();
        rslt.setData(sqlResponseStub);
        return rslt;
    }

    @Override
    public void close() throws IOException {
    }

}
