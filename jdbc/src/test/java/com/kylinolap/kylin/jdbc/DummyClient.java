package com.kylinolap.kylin.jdbc;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.ColumnMetaData.Rep;
import net.hydromatic.linq4j.Enumerator;

import com.kylinolap.kylin.jdbc.KylinMetaImpl.MetaTable;
import com.kylinolap.kylin.jdbc.stub.ConnectionException;
import com.kylinolap.kylin.jdbc.stub.DataSet;
import com.kylinolap.kylin.jdbc.stub.MetaProject;
import com.kylinolap.kylin.jdbc.stub.RemoteClient;

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
    public MetaProject getMetadata(String project) throws ConnectionException {
        List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        for (int i = 0; i < 10; i++) {
            meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        }

        List<MetaTable> tables = new ArrayList<MetaTable>();
        MetaTable table =
                new MetaTable("dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy",
                        "dummy", "dummy");
        tables.add(table);

        return new MetaProject(null, null, new DataSet<MetaTable>(meta, new KylinEnumerator<MetaTable>(
                tables.iterator())), null);
    }

    @Override
    public DataSet<Object[]> query(AvaticaStatement statement, String sql) {
        List<Object[]> data = new ArrayList<Object[]>();
        Object[] row = new Object[] { "foo", "bar", "tool" };
        data.add(row);
        Enumerator<Object[]> enumerator = new KylinEnumerator<Object[]>(data.iterator());
        List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        meta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));

        return new DataSet<Object[]>(meta, enumerator);
    }

}
