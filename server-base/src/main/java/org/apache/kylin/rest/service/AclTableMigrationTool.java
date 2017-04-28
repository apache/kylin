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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.rest.security.AclHBaseStorage;
import org.apache.kylin.rest.util.Serializer;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.HBaseResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class AclTableMigrationTool {

    private static final Serializer<LegacyAclService.SidInfo> sidSerializer = new Serializer<LegacyAclService.SidInfo>(LegacyAclService.SidInfo.class);

    private static final Serializer<LegacyAclService.DomainObjectInfo> domainObjSerializer = new Serializer<LegacyAclService.DomainObjectInfo>(LegacyAclService.DomainObjectInfo.class);

    private static final Serializer<LegacyAclService.AceInfo> aceSerializer = new Serializer<LegacyAclService.AceInfo>(LegacyAclService.AceInfo.class);

    private static final Serializer<LegacyUserService.UserGrantedAuthority[]> ugaSerializer = new Serializer<LegacyUserService.UserGrantedAuthority[]>(LegacyUserService.UserGrantedAuthority[].class);

    public static final String MIGRATE_OK_PREFIX = AclService.DIR_PREFIX + "MIGRATE_OK_";

    private static final Logger logger = LoggerFactory.getLogger(AclTableMigrationTool.class);

    public void migrate(KylinConfig kylinConfig) throws IOException {
        if (!checkIfNeedMigrate(kylinConfig)) {
            logger.info("Do not need to migrate acl table data");
            return;
        } else {
            if (!kylinConfig.getServerMode().equals("all")) {
                throw new IllegalStateException("Please make sure that you have config kylin.server.mode=all before migrating data");
            }
            logger.info("Start to migrate acl table data");
            ResourceStore store = ResourceStore.getStore(kylinConfig);
            String userTableName = kylinConfig.getMetadataUrlPrefix() + AclHBaseStorage.USER_TABLE_NAME;
            //System.out.println("user table name : " + userTableName);
            String aclTableName = kylinConfig.getMetadataUrlPrefix() + AclHBaseStorage.ACL_TABLE_NAME;
            if (needMigrateTable(aclTableName, store)) {
                logger.info("Migrate table : {}", aclTableName);
                migrate(store, AclHBaseStorage.ACL_TABLE_NAME, kylinConfig);
            }
            if (needMigrateTable(userTableName, store)) {
                logger.info("Migrate table : {}", userTableName);
                migrate(store, AclHBaseStorage.USER_TABLE_NAME, kylinConfig);
            }
        }
    }

    public boolean checkIfNeedMigrate(KylinConfig kylinConfig) throws IOException {
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        if (!(store instanceof HBaseResourceStore)) {
            logger.info("HBase enviroment not found. Not necessary to migrate data");
            return false;
        }

        String userTableName = kylinConfig.getMetadataUrlPrefix() + AclHBaseStorage.USER_TABLE_NAME;
        String aclTableName = kylinConfig.getMetadataUrlPrefix() + AclHBaseStorage.ACL_TABLE_NAME;
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
        case AclHBaseStorage.ACL_TABLE_NAME:
            String aclTableName = kylinConfig.getMetadataUrlPrefix() + AclHBaseStorage.ACL_TABLE_NAME;
            convertToResourceStore(kylinConfig, aclTableName, store, new ResultConverter() {
                @Override
                public void convertResult(ResultScanner rs, ResourceStore store) throws IOException {
                    if (rs == null)
                        return;
                    Result result = rs.next();
                    while (result != null) {
                        AclRecord record = new AclRecord();
                        DomainObjectInfo object = getDomainObjectInfoFromRs(result);
                        record.setDomainObjectInfo(object);
                        record.setParentDomainObjectInfo(getParentDomainObjectInfoFromRs(result));
                        record.setOwnerInfo(getOwnerSidInfo(result));
                        record.setEntriesInheriting(getInheriting(result));
                        record.setAllAceInfo(getAllAceInfo(result));
                        store.deleteResource(AclService.getQueryKeyById(object.getId()));
                        store.putResource(AclService.getQueryKeyById(object.getId()), record, 0, AclService.AclRecordSerializer.getInstance());
                        result = rs.next();
                    }
                }
            });
            break;
        case AclHBaseStorage.USER_TABLE_NAME:
            String userTableName = kylinConfig.getMetadataUrlPrefix() + AclHBaseStorage.USER_TABLE_NAME;

            convertToResourceStore(kylinConfig, userTableName, store, new ResultConverter() {
                @Override
                public void convertResult(ResultScanner rs, ResourceStore store) throws IOException {
                    if (rs == null)
                        return;
                    Result result = rs.next();
                    while (result != null) {
                        User user = hbaseRowToUser(result);
                        UserInfo userInfo = convert(user);
                        store.deleteResource(UserService.getId(userInfo.getUsername()));
                        store.putResource(UserService.getId(userInfo.getUsername()), userInfo, 0, UserService.UserInfoSerializer.getInstance());
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
        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        Admin hbaseAdmin = new HBaseAdmin(conf);
        return hbaseAdmin.tableExists(TableName.valueOf(tableName));
    }

    private boolean isTableAlreadyMigrate(ResourceStore store, String tableName) throws IOException {
        return store.exists(MIGRATE_OK_PREFIX + tableName);
    }

    private void convertToResourceStore(KylinConfig kylinConfig, String tableName, ResourceStore store, ResultConverter converter) throws IOException {

        Table table = null;
        ResultScanner rs = null;
        Scan scan = new Scan();
        try {
            table = HBaseConnection.get(kylinConfig.getStorageUrl()).getTable(TableName.valueOf(tableName));
            rs = table.getScanner(scan);
            converter.convertResult(rs, store);
            store.putResource(MIGRATE_OK_PREFIX + tableName, new StringEntity(tableName + " migrated"), StringEntity.serializer);
        } finally {
            IOUtils.closeQuietly(rs);
            IOUtils.closeQuietly(table);
        }

    }

    private DomainObjectInfo getDomainObjectInfoFromRs(Result result) {
        String type = String.valueOf(result.getValue(Bytes.toBytes(AclHBaseStorage.ACL_INFO_FAMILY), Bytes.toBytes(LegacyAclService.ACL_INFO_FAMILY_TYPE_COLUMN)));
        String id = String.valueOf(result.getRow());
        DomainObjectInfo newInfo = new DomainObjectInfo();
        newInfo.setId(id);
        newInfo.setType(type);
        return newInfo;
    }

    private DomainObjectInfo getParentDomainObjectInfoFromRs(Result result) throws IOException {
        LegacyAclService.DomainObjectInfo parentInfo = domainObjSerializer.deserialize(result.getValue(Bytes.toBytes(AclHBaseStorage.ACL_INFO_FAMILY), Bytes.toBytes(LegacyAclService.ACL_INFO_FAMILY_PARENT_COLUMN)));
        return convert(parentInfo);
    }

    private boolean getInheriting(Result result) {
        boolean entriesInheriting = Bytes.toBoolean(result.getValue(Bytes.toBytes(AclHBaseStorage.ACL_INFO_FAMILY), Bytes.toBytes(LegacyAclService.ACL_INFO_FAMILY_ENTRY_INHERIT_COLUMN)));
        return entriesInheriting;
    }

    private SidInfo getOwnerSidInfo(Result result) throws IOException {
        LegacyAclService.SidInfo owner = sidSerializer.deserialize(result.getValue(Bytes.toBytes(AclHBaseStorage.ACL_INFO_FAMILY), Bytes.toBytes(LegacyAclService.ACL_INFO_FAMILY_OWNER_COLUMN)));
        return convert(owner);
    }

    private Map<String, AceInfo> getAllAceInfo(Result result) throws IOException {
        Map<String, AceInfo> allAceInfoMap = new HashMap<>();
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(AclHBaseStorage.ACL_ACES_FAMILY));
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            String sid = String.valueOf(entry.getKey());
            LegacyAclService.AceInfo aceInfo = aceSerializer.deserialize(familyMap.get(entry.getValue()));
            if (null != aceInfo) {
                allAceInfoMap.put(sid, convert(aceInfo));
            }
        }
        return allAceInfoMap;
    }

    private DomainObjectInfo convert(LegacyAclService.DomainObjectInfo oldInfo) {
        if (oldInfo == null)
            return null;
        DomainObjectInfo newInfo = new DomainObjectInfo();
        newInfo.setId(String.valueOf(oldInfo.getId()));
        newInfo.setType(oldInfo.getType());
        return newInfo;
    }

    private SidInfo convert(LegacyAclService.SidInfo oldInfo) {
        if (oldInfo == null)
            return null;
        SidInfo newInfo = new SidInfo();
        newInfo.setPrincipal(oldInfo.isPrincipal());
        newInfo.setSid(oldInfo.getSid());
        return newInfo;
    }

    private AceInfo convert(LegacyAclService.AceInfo oldInfo) {
        if (oldInfo == null)
            return null;
        AceInfo newInfo = new AceInfo();
        newInfo.setPermissionMask(oldInfo.getPermissionMask());
        newInfo.setSidInfo(convert(oldInfo.getSidInfo()));
        return newInfo;
    }

    private UserInfo convert(User user) {
        if (user == null)
            return null;
        UserInfo newInfo = new UserInfo();
        newInfo.setUsername(user.getUserName());
        newInfo.setPassword(user.getPassword());
        List<String> authorities = new ArrayList<>();
        for (String auth : user.getAuthorities()) {
            authorities.add(auth);
        }
        newInfo.setAuthorities(authorities);
        return newInfo;
    }

    private User hbaseRowToUser(Result result) throws JsonParseException, JsonMappingException, IOException {
        if (null == result || result.isEmpty())
            return null;

        String username = Bytes.toString(result.getRow());

        byte[] valueBytes = result.getValue(Bytes.toBytes(AclHBaseStorage.USER_AUTHORITY_FAMILY), Bytes.toBytes(AclHBaseStorage.USER_AUTHORITY_COLUMN));
        LegacyUserService.UserGrantedAuthority[] deserialized = ugaSerializer.deserialize(valueBytes);

        String password = "";
        List<LegacyUserService.UserGrantedAuthority> authorities = Collections.emptyList();

        // password is stored at [0] of authorities for backward compatibility
        if (deserialized != null) {
            if (deserialized.length > 0 && deserialized[0].getAuthority().startsWith(LegacyUserService.PWD_PREFIX)) {
                password = deserialized[0].getAuthority().substring(LegacyUserService.PWD_PREFIX.length());
                authorities = Arrays.asList(deserialized).subList(1, deserialized.length);
            } else {
                authorities = Arrays.asList(deserialized);
            }
        }
        List<String> authoritiesStr = new ArrayList<>();
        for (LegacyUserService.UserGrantedAuthority auth : authorities) {
            if (auth != null) {
                authoritiesStr.add(auth.getAuthority());
            }
        }
        return new User(username, password, authoritiesStr);
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
