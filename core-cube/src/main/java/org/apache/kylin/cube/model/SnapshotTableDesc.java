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

package org.apache.kylin.cube.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class
SnapshotTableDesc implements java.io.Serializable{
    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("storage_type")
    private String storageType = "metaStore";

    @JsonProperty("local_cache_enable")
    private boolean enableLocalCache = true;

    @JsonProperty("global")
    private boolean global = false;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public boolean isGlobal() {
        return global;
    }

    public void setGlobal(boolean global) {
        this.global = global;
    }

//    public boolean isExtSnapshotTable() {
//        return !SnapshotTable.STORAGE_TYPE_METASTORE.equals(storageType);
//    }
//
//    public boolean isEnableLocalCache() {
//        return enableLocalCache;
//    }
//
//    public void setEnableLocalCache(boolean enableLocalCache) {
//        this.enableLocalCache = enableLocalCache;
//    }

    public static SnapshotTableDesc getCopyOf(SnapshotTableDesc other) {
        SnapshotTableDesc copy = new SnapshotTableDesc();
        copy.tableName = other.tableName;
        copy.storageType = other.storageType;
        copy.enableLocalCache = other.enableLocalCache;
        copy.global = other.global;
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotTableDesc that = (SnapshotTableDesc) o;
        return enableLocalCache == that.enableLocalCache &&
                global == that.global &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(storageType, that.storageType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, storageType, enableLocalCache, global);
    }
}
