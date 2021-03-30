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

package org.apache.kylin.storage.jdbc;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.JDBCConnectionManager;
import org.apache.kylin.common.persistence.JDBCResourceStore;
import org.apache.kylin.common.persistence.JDBCSqlQueryFormat;
import org.apache.kylin.common.persistence.JDBCSqlQueryFormatProvider;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceStoreTest;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.log4j.component.helpers.MessageFormatter;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.text.FieldPosition;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ITJDBCResourceStoreTest extends HBaseMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ITJDBCResourceStoreTest.class);

    private static final String LARGE_CELL_PATH = "/cube/_test_large_cell.json";
    private static final String Large_Content = "THIS_IS_A_LARGE_CELL";
    private KylinConfig kylinConfig;
    private JDBCConnectionManager connectionManager;
    private final String jdbcMetadataUrlNoIdentifier = "@jdbc,url=jdbc:mysql://localhost:3306/kylin_it,username=root,password=,maxActive=10,maxIdle=10";
    private final String mainIdentifier = "kylin_default_instance";
    private final String copyIdentifier = "kylin_default_instance_copy";
    private StorageURL metadataUrlBackup;
    private boolean jdbcConnectable = false;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        KylinConfig configBackup = KylinConfig.createKylinConfig(kylinConfig);
        Statement statement = null;
        Connection conn = null;
        metadataUrlBackup = kylinConfig.getMetadataUrl();
        kylinConfig.setMetadataUrl(mainIdentifier + jdbcMetadataUrlNoIdentifier);
        JDBCSqlQueryFormat sqlQueryFormat = JDBCSqlQueryFormatProvider
                .createJDBCSqlQueriesFormat(KylinConfig.getInstanceFromEnv().getMetadataDialect());
        try {
            connectionManager = JDBCConnectionManager.getConnectionManager();
            conn = connectionManager.getConn();
            statement = conn.createStatement();
            String sql = new MessageFormat(sqlQueryFormat.getTestDropSql(), Locale.ROOT)
                    .format(mainIdentifier, new StringBuffer(), new FieldPosition(0)).toString();
            statement.executeUpdate(sql);
            sql = new MessageFormat(sqlQueryFormat.getTestDropSql(), Locale.ROOT)
                    .format(copyIdentifier, new StringBuffer(), new FieldPosition(0)).toString();
            statement.executeUpdate(sql);
            jdbcConnectable = true;
            new ResourceTool().copy(configBackup, kylinConfig);
        } catch (RuntimeException ex) {
            logger.info("Init connection manager failed, skip test cases");
        } finally {
            JDBCConnectionManager.closeQuietly(statement);
            JDBCConnectionManager.closeQuietly(conn);
        }
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        kylinConfig.setMetadataUrl(metadataUrlBackup.toString());
    }

    @Test
    public void testConnectJDBC() throws Exception {
        Assume.assumeTrue(jdbcConnectable);
        Connection conn = null;
        try {
            conn = connectionManager.getConn();
            assertNotNull(conn);
        } finally {
            JDBCConnectionManager.closeQuietly(conn);
        }
    }

    @Test
    public void testJdbcBasicFunction() throws Exception {
        Assume.assumeTrue(jdbcConnectable);
        Connection conn = null;
        Statement statement = null;
        String createTableSql = "CREATE TABLE test(col1 VARCHAR (10), col2 INTEGER )";
        String dropTableSql = "DROP TABLE IF EXISTS test";
        try {
            conn = connectionManager.getConn();
            statement = conn.createStatement();
            statement.executeUpdate(dropTableSql);
            statement.executeUpdate(createTableSql);
            statement.executeUpdate(dropTableSql);
        } finally {
            JDBCConnectionManager.closeQuietly(statement);
            JDBCConnectionManager.closeQuietly(conn);
        }
    }

    @Test
    public void testMsgFormatter() {
        System.out.println(MessageFormatter.format("{}:{}", "a", "b"));
    }

    @Test
    public void testResourceStoreBasic() throws Exception {
        Assume.assumeTrue(jdbcConnectable);
        ResourceStoreTest.testAStore(
                ResourceStoreTest.mockUrl(
                        StringUtils.substringAfterLast(mainIdentifier + jdbcMetadataUrlNoIdentifier, "@"), kylinConfig),
                kylinConfig);
    }

    @Test
    public void testJDBCStoreWithLargeCell() throws Exception {
        Assume.assumeTrue(jdbcConnectable);
        JDBCResourceStore store = null;
        StringEntity content = new StringEntity(Large_Content);
        String largePath = "/large/large.json";
        try {
            String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig,
                    ResourceStoreTest.mockUrl("jdbc", kylinConfig));
            store = new JDBCResourceStore(KylinConfig.getInstanceFromEnv());
            store.deleteResource(largePath);
            store.checkAndPutResource(largePath, content, StringEntity.serializer);
            assertTrue(store.exists(largePath));
            StringEntity t = store.getResource(largePath, StringEntity.serializer);
            assertEquals(content, t);
            store.deleteResource(LARGE_CELL_PATH);
            ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
        } finally {
            if (store != null)
                store.deleteResource(LARGE_CELL_PATH);
        }
    }

    @Test
    public void testPerformance() throws Exception {
        Assume.assumeTrue(jdbcConnectable);
        ResourceStoreTest.testPerformance(ResourceStoreTest.mockUrl("jdbc", kylinConfig), kylinConfig);
        ResourceStoreTest.testPerformance(ResourceStoreTest.mockUrl("hbase", kylinConfig), kylinConfig);
    }

    @Test
    public void testMaxCell() throws Exception {
        Assume.assumeTrue(jdbcConnectable);
        byte[] data = new byte[500 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) 0;
        }
        JDBCResourceStore store = null;
        ByteEntity content = new ByteEntity(data);
        try {
            String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig,
                    ResourceStoreTest.mockUrl("jdbc", kylinConfig));
            store = new JDBCResourceStore(KylinConfig.getInstanceFromEnv());
            store.deleteResource(LARGE_CELL_PATH);
            store.checkAndPutResource(LARGE_CELL_PATH, content, ByteEntity.serializer);
            assertTrue(store.exists(LARGE_CELL_PATH));
            ByteEntity t = store.getResource(LARGE_CELL_PATH, ByteEntity.serializer);
            assertEquals(content, t);
            store.deleteResource(LARGE_CELL_PATH);
            ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
        } finally {
            if (store != null)
                store.deleteResource(LARGE_CELL_PATH);
        }
    }

    @Test
    public void testPerformanceWithResourceTool() throws Exception {
        Assume.assumeTrue(jdbcConnectable);
        KylinConfig tmpConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        tmpConfig.setMetadataUrl(copyIdentifier + jdbcMetadataUrlNoIdentifier);

        JDBCResourceStore store = (JDBCResourceStore) ResourceStore.getStore(kylinConfig);
        NavigableSet<String> executes = store.listResources(ResourceStore.EXECUTE_RESOURCE_ROOT);
        NavigableSet<String> executeOutputs = store.listResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT);

        long startTs = System.currentTimeMillis();

        for (String execute : executes) {
            String uuid = StringUtils.substringAfterLast(execute, "/");
            RawResource executeResource = store.getResource(execute);
            Map<String, RawResource> executeOutputResourceMap = new HashMap<>();

            for (String executeOutput : executeOutputs) {
                if (executeOutput.contains(uuid)) {
                    RawResource executeOutputResource = store.getResource(executeOutput);
                    executeOutputResourceMap.put(executeOutput, executeOutputResource);
                }
            }

            for (int i = 0; i < 200; i++) {
                String newUuid = UUID.randomUUID().toString();
                store.putResource(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + newUuid, executeResource.content(),
                        System.currentTimeMillis());

                for (String key : executeOutputResourceMap.keySet()) {
                    String step = StringUtils.substringAfterLast(key, uuid);
                    store.putResource(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + newUuid + step,
                            executeOutputResourceMap.get(key).content(), System.currentTimeMillis());
                }
            }
        }

        long queryNumBeforeCopy = store.getQueriedSqlNum();
        new ResourceTool().copy(kylinConfig, tmpConfig);
        long endTs = System.currentTimeMillis();
        long queryNumAfterCopy = store.getQueriedSqlNum();
        JDBCResourceStore resourceStoreCopy = (JDBCResourceStore) ResourceStore.getStore(tmpConfig);

        int executeNum = store.listResources("/execute").size();
        int executeOutputNum = store.listResources("/execute_output").size();

        assertEquals(executeNum, resourceStoreCopy.listResources("/execute").size());
        assertEquals(executeOutputNum, resourceStoreCopy.listResources("/execute_output").size());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
        String startTime = sdf.format(new Date(Long.parseLong(String.valueOf(startTs))));
        String endTime = sdf.format(new Date(Long.parseLong(String.valueOf(endTs))));

        logger.info("Test performance with ResourceTool done during " + startTime + " to " + endTime);
        logger.info("Now there is " + executeNum + " execute data and " + executeOutputNum
                + " execute_output data in resource store.");
        logger.info("Resource store run " + queryNumBeforeCopy + " sqls for metadata generation, and "
                + (queryNumAfterCopy - queryNumBeforeCopy) + " sqls for copy with ResourceTool.");
        assertTrue((queryNumAfterCopy - queryNumBeforeCopy) < queryNumBeforeCopy);
        logger.info("This test is expected to be done in 10 mins.");
        assertTrue((endTs - startTs) < 600000);
    }

    @SuppressWarnings("serial")
    public static class ByteEntity extends RootPersistentEntity {

        public static final Serializer<ByteEntity> serializer = new Serializer<ByteEntity>() {

            @Override
            public void serialize(ByteEntity obj, DataOutputStream out) throws IOException {
                byte[] data = obj.getData();
                out.writeInt(data.length);
                out.write(data);
            }

            @Override
            public ByteEntity deserialize(DataInputStream in) throws IOException {
                int length = in.readInt();
                byte[] bytes = new byte[length];
                in.read(bytes);
                return new ByteEntity(bytes);
            }
        };
        byte[] data;

        public ByteEntity() {

        }

        public ByteEntity(byte[] data) {
            this.data = data;
        }

        public static Serializer<ByteEntity> getSerializer() {
            return serializer;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }
    }
}
