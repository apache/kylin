package org.apache.kylin.source.hive;

import java.io.File;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJdbcExplorer {
    private static Logger logger = LoggerFactory.getLogger(TestJdbcExplorer.class);
    public static String TEST_DATA = "../examples/jdbc_case_data/sandbox";
    @Test
    public void testGetDbNames() throws Exception{
        ClassUtil.addClasspath(new File(TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, TEST_DATA);
        JdbcExplorer jdbcClient = new JdbcExplorer();
        List<String> schemas = jdbcClient.listDatabases();
        logger.info(schemas.toString());
    }
    
    @Test
    public void testGetTables() throws Exception{
        ClassUtil.addClasspath(new File(TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, TEST_DATA);
        JdbcExplorer jdbcClient = new JdbcExplorer();
        List<String> tables = jdbcClient.listTables("kylin");
        logger.info(tables.toString());
    }
    
    @Test
    public void testGetTableType() throws Exception{
        ClassUtil.addClasspath(new File(TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, TEST_DATA);
        
        JdbcExplorer jdbcClient = new JdbcExplorer();
        Pair<TableDesc, TableExtDesc> tableDescs = jdbcClient.loadTableMetadata("kylin","KYLIN_SALES");
        logger.info(tableDescs.toString());
    }
}
