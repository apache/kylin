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

package org.apache.kylin.dict.lookup;

import java.io.IOException;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.source.IReadableTable.TableSignature;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ExtTableSnapshotInfo extends RootPersistentEntity {
    public static final String STORAGE_TYPE_HBASE = "hbase";

    @JsonProperty("tableName")
    private String tableName;

    @JsonProperty("signature")
    private TableSignature signature;

    @JsonProperty("key_columns")
    private String[] keyColumns;

    @JsonProperty("storage_type")
    private String storageType;

    @JsonProperty("storage_location_identifier")
    private String storageLocationIdentifier;

    @JsonProperty("shard_num")
    private int shardNum;

    @JsonProperty("row_cnt")
    private long rowCnt;

    @JsonProperty("last_build_time")
    private long lastBuildTime;

    // default constructor for JSON serialization
    public ExtTableSnapshotInfo() {
    }

    public ExtTableSnapshotInfo(TableSignature signature, String tableName) throws IOException {
        this.signature = signature;
        this.tableName = tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getResourcePath() {
        return getResourcePath(tableName, uuid);
    }

    public String getResourceDir() {
        return getResourceDir(tableName);
    }

    public static String getResourcePath(String tableName, String uuid) {
        return getResourceDir(tableName) + "/" + uuid + ".snapshot";
    }

    public static String getResourceDir(String tableName) {
        return ResourceStore.EXT_SNAPSHOT_RESOURCE_ROOT + "/" + tableName;
    }

    public TableSignature getSignature() {
        return signature;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public String getStorageLocationIdentifier() {
        return storageLocationIdentifier;
    }

    public void setStorageLocationIdentifier(String storageLocationIdentifier) {
        this.storageLocationIdentifier = storageLocationIdentifier;
    }

    public String[] getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(String[] keyColumns) {
        this.keyColumns = keyColumns;
    }

    public int getShardNum() {
        return shardNum;
    }

    public void setShardNum(int shardNum) {
        this.shardNum = shardNum;
    }

    public String getTableName() {
        return tableName;
    }

    public void setSignature(TableSignature signature) {
        this.signature = signature;
    }

    public long getRowCnt() {
        return rowCnt;
    }

    public void setRowCnt(long rowCnt) {
        this.rowCnt = rowCnt;
    }

    public long getLastBuildTime() {
        return lastBuildTime;
    }

    public void setLastBuildTime(long lastBuildTime) {
        this.lastBuildTime = lastBuildTime;
    }

}
