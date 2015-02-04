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

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.ColumnMetaData.Rep;
import net.hydromatic.linq4j.Enumerator;

import org.apache.kylin.jdbc.stub.ConnectionException;
import org.apache.kylin.jdbc.stub.DataSet;
import org.apache.kylin.jdbc.stub.RemoteClient;

/**
 * @author xduo
 * 
 */
public class DummyClient implements RemoteClient {

    public DummyClient(KylinConnectionImpl conn) {
    }

    @Override
    public void connect() throws ConnectionException {
    }

    @Override
    public KylinMetaImpl.MetaProject getMetadata(String project) throws ConnectionException {
        List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        for (int i = 0; i < 10; i++) {
            meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        }

        List<KylinMetaImpl.MetaTable> tables = new ArrayList<KylinMetaImpl.MetaTable>();
        KylinMetaImpl.MetaTable table = new KylinMetaImpl.MetaTable("dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy", new ArrayList<KylinMetaImpl.MetaColumn>());
        tables.add(table);

        List<KylinMetaImpl.MetaSchema> schemas = new ArrayList<KylinMetaImpl.MetaSchema>();
        schemas.add(new KylinMetaImpl.MetaSchema("dummy", "dummy", tables));
        List<KylinMetaImpl.MetaCatalog> catalogs = new ArrayList<KylinMetaImpl.MetaCatalog>();
        catalogs.add(new KylinMetaImpl.MetaCatalog("dummy", schemas));

        return new KylinMetaImpl.MetaProject(null, catalogs);
    }

    @Override
    public DataSet<Object[]> query(AvaticaStatement statement, String sql) {
        List<Object[]> data = new ArrayList<Object[]>();
        Object[] row = new Object[] { "foo", "bar", "tool" };
        data.add(row);
        Enumerator<Object[]> enumerator = new KylinEnumerator<Object[]>(data);
        List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));

        return new DataSet<Object[]>(meta, enumerator);
    }

}
