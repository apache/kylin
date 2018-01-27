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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.rest.security.AclConstant;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.security.springacl.AclRecord;
import org.apache.kylin.rest.security.springacl.LegacyAceInfo;
import org.apache.kylin.rest.security.springacl.ObjectIdentityImpl;
import org.apache.kylin.rest.security.springacl.SidInfo;
import org.apache.kylin.rest.util.Serializer;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.HBaseResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class AclTableMigrationTool {

    private static final Serializer<SidInfo> sidSerializer = new Serializer<SidInfo>(SidInfo.class);

    private static final Serializer<ObjectIdentityImpl> domainObjSerializer = new Serializer<ObjectIdentityImpl>(
            ObjectIdentityImpl.class);

    private static final Serializer<LegacyAceInfo> aceSerializer = new Serializer<LegacyAceInfo>(LegacyAceInfo.class);

    private static final Serializer<UserGrantedAuthority[]> ugaSerializer = new Serializer<UserGrantedAuthority[]>(
            UserGrantedAuthority[].class);

    public static final String MIGRATE_OK_PREFIX = "MIGRATE_OK_";

    private static final Logger logger = LoggerFactory.getLogger(AclTableMigrationTool.class);

    public void migrate(KylinConfig kylinConfig) throws IOException {
        if (!checkIfNeedMigrate(kylinConfig)) {
            logger.info("Do not need to migrate acl table data");
            return;
        } else {
            if (!kylinConfig.getServerMode().equals("all")) {
                throw new IllegalStateException(
                        "Please make sure that you have config kylin.server.mode=all before migrating data");
            }
            logger.info("Start to migrate acl table data");
            ResourceStore store = ResourceStore.getStore(kylinConfig);
            String userTableName = kylinConfig.getMetadataUrlPrefix() + AclConstant.USER_TABLE_NAME;
            //System.out.println("user table name : " + userTableName);
            String aclTableName = kylinConfig.getMetadataUrlPrefix() + AclConstant.ACL_TABLE_NAME;
            if (needMigrateTable(aclTableName, store)) {
                logger.info("Migrate table : {}", aclTableName);
                migrate(store, AclConstant.ACL_TABLE_NAME, kylinConfig);
            }
            if (needMigrateTable(userTableName, store)) {
                logger.info("Migrate table : {}", userTableName);
                migrate(store, AclConstant.USER_TABLE_NAME, kylinConfig);
            }
        }
    }

    public boolean checkIfNeedMigrate(KylinConfig kylinConfig) throws IOException {
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        if (!(store instanceof HBaseResourceStore)) {
            logger.info("HBase enviroment not found. Not necessary to migrate data");
            return false;
        }

        String userTableName = kylinConfig.getMetadataUrlPrefix() + AclConstant.USER_TABLE_NAME;
        String aclTableName = kylinConfig.getMetadataUrlPrefix() + AclConstant.ACL_TABLE_NAME;
        if (needMigrateTable(aclTableName, store) || needMigrateTable(userTableName, store))
            return true;
        return false;

    }

    private boolean needMigrateTable(String tableName, ResourceStore store) throws IOException {
        if (checkTableExist(tableName) && !isTableAlreadyMigrate(store, tableName))
            return true;
        return false;
    }

    private void migrate(ResourceStore store, String tableType, KylinConfig kylinConfig) throws IOException {

        switch (tableType) {
        case AclConstant.ACL_TABLE_NAME:
            String aclTableName = kylinConfig.getMetadataUrlPrefix() + AclConstant.ACL_TABLE_NAME;
            convertToResourceStore(kylinConfig, aclTableName, store, new ResultConverter() {
                @Override
                public void convertResult(ResultScanner rs, ResourceStore store) throws IOException {
                    if (rs == null)
                        return;
                    Result result = rs.next();
                    while (result != null) {
                        AclRecord record = new AclRecord();
                        ObjectIdentityImpl object = getDomainObjectInfoFromRs(result);
                        record.setDomainObjectInfo(object);
                        record.setParentDomainObjectInfo(getParentDomainObjectInfoFromRs(result));
                        record.setOwnerInfo(getOwnerSidInfo(result));
                        record.setEntriesInheriting(getInheriting(result));
                        record.setAllAceInfo(getAllAceInfo(result));
                        store.putResourceWithoutCheck(AclService.resourceKey(object.getId()), record,
                                System.currentTimeMillis(), AclService.SERIALIZER);
                        result = rs.next();
                    }
                }
            });
            break;
        case AclConstant.USER_TABLE_NAME:
            String userTableName = kylinConfig.getMetadataUrlPrefix() + AclConstant.USER_TABLE_NAME;

            convertToResourceStore(kylinConfig, userTableName, store, new ResultConverter() {
                @Override
                public void convertResult(ResultScanner rs, ResourceStore store) throws IOException {
                    if (rs == null)
                        return;
                    Result result = rs.next();
                    while (result != null) {
                        ManagedUser user = hbaseRowToUser(result);
                        store.putResourceWithoutCheck(KylinUserService.getId(user.getUsername()), user,
                                System.currentTimeMillis(), KylinUserService.SERIALIZER);
                        result = rs.next();
                    }
                }
            });
            break;
        default:
            throw new IOException("Unrecognized table type");

        }

    }

    private boolean checkTableExist(String tableName) throws IOException {
        StorageURL metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        try (Admin admin = HBaseConnection.get(metadataUrl).getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        }
    }

    private boolean isTableAlreadyMigrate(ResourceStore store, String tableName) throws IOException {
        return store.exists(MIGRATE_OK_PREFIX + tableName);
    }

    private void convertToResourceStore(KylinConfig kylinConfig, String tableName, ResourceStore store,
            ResultConverter converter) throws IOException {

        Table table = null;
        ResultScanner rs = null;
        Scan scan = new Scan();
        try {
            table = HBaseConnection.get(kylinConfig.getStorageUrl()).getTable(TableName.valueOf(tableName));
            rs = table.getScanner(scan);
            converter.convertResult(rs, store);
            store.putResource(MIGRATE_OK_PREFIX + tableName, new StringEntity(tableName + " migrated"),
                    StringEntity.serializer);
        } finally {
            IOUtils.closeQuietly(rs);
            IOUtils.closeQuietly(table);
        }

    }

    private ObjectIdentityImpl getDomainObjectInfoFromRs(Result result) {
        String type = new String(result.getValue(Bytes.toBytes(AclConstant.ACL_INFO_FAMILY),
                Bytes.toBytes(AclConstant.ACL_INFO_FAMILY_TYPE_COLUMN)));
        String id = new String(result.getRow());
        ObjectIdentityImpl newInfo = new ObjectIdentityImpl(type, id);
        return newInfo;
    }

    private ObjectIdentityImpl getParentDomainObjectInfoFromRs(Result result) throws IOException {
        ObjectIdentityImpl parentInfo = domainObjSerializer.deserialize(result.getValue(
                Bytes.toBytes(AclConstant.ACL_INFO_FAMILY), Bytes.toBytes(AclConstant.ACL_INFO_FAMILY_PARENT_COLUMN)));
        return parentInfo;
    }

    private boolean getInheriting(Result result) {
        boolean entriesInheriting = Bytes.toBoolean(result.getValue(Bytes.toBytes(AclConstant.ACL_INFO_FAMILY),
                Bytes.toBytes(AclConstant.ACL_INFO_FAMILY_ENTRY_INHERIT_COLUMN)));
        return entriesInheriting;
    }

    private SidInfo getOwnerSidInfo(Result result) throws IOException {
        SidInfo owner = sidSerializer.deserialize(result.getValue(Bytes.toBytes(AclConstant.ACL_INFO_FAMILY),
                Bytes.toBytes(AclConstant.ACL_INFO_FAMILY_OWNER_COLUMN)));
        return owner;
    }

    private Map<String, LegacyAceInfo> getAllAceInfo(Result result) throws IOException {
        Map<String, LegacyAceInfo> allAceInfoMap = new HashMap<>();
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(AclConstant.ACL_ACES_FAMILY));

        if (familyMap != null && !familyMap.isEmpty()) {
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                String sid = new String(entry.getKey());
                LegacyAceInfo aceInfo = aceSerializer.deserialize(entry.getValue());
                if (null != aceInfo) {
                    allAceInfoMap.put(sid, aceInfo);
                }
            }
        }
        return allAceInfoMap;
    }

    private ManagedUser hbaseRowToUser(Result result) throws JsonParseException, JsonMappingException, IOException {
        if (null == result || result.isEmpty())
            return null;

        String username = Bytes.toString(result.getRow());

        byte[] valueBytes = result.getValue(Bytes.toBytes(AclConstant.USER_AUTHORITY_FAMILY),
                Bytes.toBytes(AclConstant.USER_AUTHORITY_COLUMN));
        UserGrantedAuthority[] deserialized = ugaSerializer.deserialize(valueBytes);

        String password = "";
        List<UserGrantedAuthority> authorities = Collections.emptyList();

        // password is stored at [0] of authorities for backward compatibility
        if (deserialized != null) {
            if (deserialized.length > 0 && deserialized[0].getAuthority().startsWith(AclConstant.PWD_PREFIX)) {
                password = deserialized[0].getAuthority().substring(AclConstant.PWD_PREFIX.length());
                authorities = Arrays.asList(deserialized).subList(1, deserialized.length);
            } else {
                authorities = Arrays.asList(deserialized);
            }
        }
        return new ManagedUser(username, password, false, authorities);
    }

    interface ResultConverter {
        void convertResult(ResultScanner rs, ResourceStore store) throws IOException;
    }

    public static class User {
        String userName;

        String password;

        List<String> authorities;

        public User(String userName, String password, List<String> authorities) {
            this.userName = userName;
            this.password = password;
            this.authorities = authorities;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public List<String> getAuthorities() {
            return authorities;
        }

        public void setAuthorities(List<String> authorities) {
            this.authorities = authorities;
        }
    }
}
