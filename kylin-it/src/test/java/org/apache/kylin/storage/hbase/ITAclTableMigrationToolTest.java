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

package org.apache.kylin.storage.hbase;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceStoreTest;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.security.AclConstant;
import org.apache.kylin.rest.service.AclService;
import org.apache.kylin.rest.service.AclTableMigrationTool;
import org.apache.kylin.rest.service.KylinUserService;
import org.apache.kylin.rest.util.Serializer;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.fasterxml.jackson.core.JsonProcessingException;

public class ITAclTableMigrationToolTest extends HBaseMetadataTestCase {

    private KylinConfig kylinConfig;

    private String STORE_WITH_OLD_TABLE = "STORE_WITH_OLD_TABLE";

    private String STORE_WITHOUT_OLD_TABLE = "STORE_WITHOUT_OLD_TABLE";

    private Logger logger = LoggerFactory.getLogger(ITAclTableMigrationToolTest.class);

    private TableName aclTable = TableName.valueOf(STORE_WITH_OLD_TABLE + AclConstant.ACL_TABLE_NAME);

    private TableName userTable = TableName.valueOf(STORE_WITH_OLD_TABLE + AclConstant.USER_TABLE_NAME);

    private Serializer<SimpleGrantedAuthority[]> ugaSerializer = new Serializer<>(SimpleGrantedAuthority[].class);

    private AclTableMigrationTool aclTableMigrationJob;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();

        // if not HBaseResourceStore, the following tests will be skipped
        Assume.assumeTrue((ResourceStore.getStore(kylinConfig) instanceof HBaseResourceStore));

        cleanUpAll();
        createTestHTables();
        addRecordsToTable();
        aclTableMigrationJob = new AclTableMigrationTool();
    }

    @Test
    public void testBasic() throws Exception {
        //old table not exist
        String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig, STORE_WITHOUT_OLD_TABLE + "@hbase");
        boolean check = aclTableMigrationJob.checkIfNeedMigrate(kylinConfig);
        assertFalse(check);
        ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);

        //old table exist

        oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig, STORE_WITH_OLD_TABLE + "@hbase");
        check = aclTableMigrationJob.checkIfNeedMigrate(kylinConfig);
        assertTrue(check);
        ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);

        //migrate test
        oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig, STORE_WITH_OLD_TABLE + "@hbase");
        aclTableMigrationJob.migrate(kylinConfig);
        check = aclTableMigrationJob.checkIfNeedMigrate(kylinConfig);
        assertFalse(check);
        ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
    }

    @After
    public void after() throws Exception {
        cleanUpAll();
        this.cleanupTestMetadata();
    }

    private void cleanUpAll() throws IOException {
        cleanUpMetastoreData(STORE_WITH_OLD_TABLE);
        cleanUpMetastoreData(STORE_WITHOUT_OLD_TABLE);
        dropTestHTables();
    }

    private void createTestHTables() throws IOException {
        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        Admin hbaseAdmin = new HBaseAdmin(conf);
        creatTable(hbaseAdmin, conf, aclTable, new String[] { AclConstant.ACL_INFO_FAMILY, AclConstant.ACL_ACES_FAMILY });
        creatTable(hbaseAdmin, conf, userTable, new String[] { AclConstant.USER_AUTHORITY_FAMILY });
        hbaseAdmin.close();
    }

    private void addRecordsToTable() throws Exception {
        Table htable = HBaseConnection.get(kylinConfig.getStorageUrl()).getTable(userTable);
        Pair<byte[], byte[]> pair = getRandomUserRecord();
        Put put = new Put(pair.getFirst());
        put.addColumn(Bytes.toBytes(AclConstant.USER_AUTHORITY_FAMILY), Bytes.toBytes(AclConstant.USER_AUTHORITY_COLUMN), pair.getSecond());
        htable.put(put);
    }

    private void cleanUpMetastoreData(String storeName) throws IOException {
        String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig, STORE_WITH_OLD_TABLE + "@hbase");
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        Set<String> allRes1 = store.listResources(KylinUserService.DIR_PREFIX);
        Set<String> allRes2 = store.listResources(AclService.DIR_PREFIX);
        if (allRes1 != null) {
            for (String res : allRes1) {
                store.deleteResource(res);
            }
        }
        if (allRes2 != null) {
            for (String res : allRes2) {
                store.deleteResource(res);
            }
        }

        ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
    }

    private void dropTestHTables() throws IOException {
        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        Admin hbaseAdmin = new HBaseAdmin(conf);
        if (hbaseAdmin.tableExists(aclTable)) {
            if (hbaseAdmin.isTableEnabled(aclTable))
                hbaseAdmin.disableTable(aclTable);
            hbaseAdmin.deleteTable(aclTable);
        }
        if (hbaseAdmin.tableExists(userTable)) {
            if (hbaseAdmin.isTableEnabled(userTable))
                hbaseAdmin.disableTable(userTable);
            hbaseAdmin.deleteTable(userTable);
        }
        hbaseAdmin.close();
    }

    private void creatTable(Admin admin, Configuration conf, TableName tableName, String[] family) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tableName)) {
            throw new IOException("Table : " + tableName + " exists");
        } else {
            admin.createTable(desc);
            logger.info("create table Success!");
        }
    }

    private Pair<byte[], byte[]> getRandomUserRecord() throws JsonProcessingException {
        byte[] key = Bytes.toBytes("username");

        Collection<? extends GrantedAuthority> authorities = new ArrayList<>();
        if (authorities == null)
            authorities = Collections.emptyList();

        SimpleGrantedAuthority[] serializing = new SimpleGrantedAuthority[authorities.size() + 1];

        // password is stored as the [0] authority
        serializing[0] = new SimpleGrantedAuthority(AclConstant.PWD_PREFIX + "password");
        int i = 1;
        for (GrantedAuthority a : authorities) {
            serializing[i++] = new SimpleGrantedAuthority(a.getAuthority());
        }

        byte[] value = ugaSerializer.serialize(serializing);
        return Pair.newPair(key, value);
    }

}
