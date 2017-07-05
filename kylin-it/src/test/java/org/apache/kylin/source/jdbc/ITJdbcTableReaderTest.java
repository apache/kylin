package org.apache.kylin.source.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.query.H2Database;
import org.apache.kylin.source.datagen.ModelDataGenerator;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.ext.h2.H2Connection;
import org.dbunit.ext.h2.H2DataTypeFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITJdbcTableReaderTest extends LocalFileMetadataTestCase implements ISourceAware {

    protected KylinConfig config = null;
    protected static Connection h2Connection = null;

    @Before
    public void setup() throws Exception {

        super.createTestMetadata();

        System.setProperty("kylin.source.jdbc.connection-url", "jdbc:h2:mem:db");
        System.setProperty("kylin.source.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.source.jdbc.user", "sa");
        System.setProperty("kylin.source.jdbc.pass", "");

        config = KylinConfig.getInstanceFromEnv();

        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db", "sa", "");

        H2Database h2DB = new H2Database(h2Connection, config);

        MetadataManager mgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        ModelDataGenerator gen = new ModelDataGenerator(mgr.getDataModelDesc("ci_left_join_model"), 10000);
        gen.generate();

        h2DB.loadAllTables();

    }

    @After
    public void after() throws Exception {

        super.cleanupTestMetadata();

        if (h2Connection != null) {
            try {
                h2Connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.clearProperty("kylin.source.jdbc.connection-url");
        System.clearProperty("kylin.source.jdbc.driver");
        System.clearProperty("kylin.source.jdbc.user");
        System.clearProperty("kylin.source.jdbc.pass");

    }

    @Test
    public void test() throws Exception {

        JdbcTableReader reader = new JdbcTableReader("default", "test_kylin_fact");
        int rowNumber = 0;
        while (reader.next()) {
            String[] row = reader.getRow();
            Assert.assertEquals(11, row.length);

            rowNumber++;
        }

        reader.close();
        Assert.assertEquals(10000, rowNumber);

    }

    @Override
    public int getSourceType() {
        return ISourceAware.ID_JDBC;
    }

    @SuppressWarnings("deprecation")
    protected static H2Connection newH2Connection() throws DatabaseUnitException {
        H2Connection h2Conn = new H2Connection(h2Connection, null);
        h2Conn.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new TestH2DataTypeFactory());
        h2Conn.getConfig().setFeature(DatabaseConfig.FEATURE_DATATYPE_WARNING, false);
        return h2Conn;
    }

    public static class TestH2DataTypeFactory extends H2DataTypeFactory {
        @Override
        public DataType createDataType(int sqlType, String sqlTypeName, String tableName, String columnName)
                throws DataTypeException {

            if ((columnName.startsWith("COL") || columnName.startsWith("col")) && sqlType == Types.BIGINT) {
                return DataType.INTEGER;
            }
            return super.createDataType(sqlType, sqlTypeName);
        }
    }

}
