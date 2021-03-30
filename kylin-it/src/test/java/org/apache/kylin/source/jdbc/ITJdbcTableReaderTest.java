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

package org.apache.kylin.source.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.H2Database;
import org.apache.kylin.source.datagen.ModelDataGenerator;
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

        System.setProperty("kylin.source.jdbc.connection-url", "jdbc:h2:mem:db" + "_jdbc_table_reader");
        System.setProperty("kylin.source.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.source.jdbc.user", "sa");
        System.setProperty("kylin.source.jdbc.pass", "");

        config = KylinConfig.getInstanceFromEnv();

        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + "_jdbc_table_reader", "sa", "");

        String project = ProjectInstance.DEFAULT_PROJECT_NAME;
        H2Database h2DB = new H2Database(h2Connection, config, project);

        DataModelManager mgr = DataModelManager.getInstance(KylinConfig.getInstanceFromEnv());
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
            Assert.assertEquals(12, row.length);

            rowNumber++;
        }

        reader.close();
        Assert.assertEquals(10000, rowNumber);

    }

    @Override
    public int getSourceType() {
        return ISourceAware.ID_JDBC;
    }

    @Override
    public KylinConfig getConfig() {
        return getTestConfig();
    }

}
