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

package org.apache.kylin.query;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.calcite.jdbc.Driver;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryDataSourceTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCreateDataSource() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        DataSource ds = QueryDataSource.create("default", config);
        Assert.assertNotNull(ds);
    }

    @Test
    public void testCreateDataSourceWithProps() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Properties props = new Properties();
        props.setProperty("username", "ADMIN");
        props.setProperty("password", "KYLIN");
        BasicDataSource ds = (BasicDataSource) QueryDataSource.create("default", config, props);

        Assert.assertNotNull(ds);
        Assert.assertTrue(ds instanceof BasicDataSource);
        Assert.assertTrue(ds.getUrl().startsWith("jdbc:calcite:model="));
        Assert.assertEquals(ds.getDriverClassName(), Driver.class.getName());
        Assert.assertEquals(ds.getUsername(), "ADMIN");
        Assert.assertEquals(ds.getPassword(), "KYLIN");
    }

    @Test
    public void testGetCachedDataSource() {
        QueryDataSource dsCache = new QueryDataSource();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        DataSource ds1 = dsCache.get("default", config);
        DataSource ds2 = dsCache.get("default", config);
        dsCache.removeCache("default");
        DataSource ds3 = dsCache.get("default", config);

        Assert.assertNotNull(ds1);
        Assert.assertNotNull(ds2);
        Assert.assertNotNull(ds3);
        Assert.assertEquals(ds1, ds2);
        Assert.assertNotEquals(ds1, ds3);

        dsCache.clearCache();
    }

    @Test(timeout=10000)
    public void testMaxConnLimit() throws SQLException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        
        // Test with connection limit 
        Properties props = new Properties();
        props.setProperty("maxActive", "3");
        props.setProperty("maxWait", "1000");
        DataSource ds1 = QueryDataSource.create("default", config, props);
        Connection ds1Conn1 = ds1.getConnection();
        Connection ds1Conn2 = ds1.getConnection();
        Connection ds1Conn3 = ds1.getConnection();
        try {
            Connection ds1Conn4 = ds1.getConnection();
            DBUtils.closeQuietly(ds1Conn4);
            Assert.fail("This should fail with SQLException");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
        }
        DBUtils.closeQuietly(ds1Conn1);
        DBUtils.closeQuietly(ds1Conn2);
        DBUtils.closeQuietly(ds1Conn3);
        
        // Test with not connection limit
        DataSource ds2 = QueryDataSource.create("default", config);
        Connection ds2Conn1 = ds2.getConnection();
        Connection ds2Conn2 = ds2.getConnection();
        Connection ds2Conn3 = ds2.getConnection();
        Connection ds2Conn4 = ds2.getConnection();
        Connection ds2Conn5 = ds2.getConnection();
        Connection ds2Conn6 = ds2.getConnection();
        Connection ds2Conn7 = ds2.getConnection();
        Connection ds2Conn8 = ds2.getConnection(); // DBCP default is 8, should be bypassed with no limit
        Connection ds2Conn9 = ds2.getConnection();
        DBUtils.closeQuietly(ds2Conn1);
        DBUtils.closeQuietly(ds2Conn2);
        DBUtils.closeQuietly(ds2Conn3);
        DBUtils.closeQuietly(ds2Conn4);
        DBUtils.closeQuietly(ds2Conn5);
        DBUtils.closeQuietly(ds2Conn6);
        DBUtils.closeQuietly(ds2Conn7);
        DBUtils.closeQuietly(ds2Conn8);
        DBUtils.closeQuietly(ds2Conn9);
    }
}
