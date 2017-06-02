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

package org.apache.kylin.source.hive;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJdbcExplorer {
    private static Logger logger = LoggerFactory.getLogger(TestJdbcExplorer.class);
    public static String TEST_DATA = "../examples/jdbc_case_data/sandbox";
    
    @Before
    public void setup(){
        ClassUtil.addClasspath(new File(TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, TEST_DATA);
    }
    
    @Test
    public void testGetDbNames() throws Exception{
        JdbcExplorer jdbcClient = new JdbcExplorer();
        List<String> schemas = jdbcClient.listDatabases();
        logger.info(schemas.toString());
    }
    
    @Test
    public void testGetTables() throws Exception{
        JdbcExplorer jdbcClient = new JdbcExplorer();
        List<String> tables = jdbcClient.listTables("kylin");
        logger.info(tables.toString());
    }
    
    @Test
    public void testGetTableType() throws Exception{
        JdbcExplorer jdbcClient = new JdbcExplorer();
        Pair<TableDesc, TableExtDesc> tableDescs = jdbcClient.loadTableMetadata("kylin", "KYLIN_SALES");
        logger.info(tableDescs.toString());
    }
    
    @Test
    public void testJDBCTableReader() throws Exception{
        JdbcTableReader reader = new JdbcTableReader("kylin", "KYLIN_SALES");
        try {
            while (reader.next()) {
                String[] vs = reader.getRow();
                logger.info(String.format("row:%s", Arrays.asList(vs)));
            }
        } finally {
            reader.close();
        }
    }
}
