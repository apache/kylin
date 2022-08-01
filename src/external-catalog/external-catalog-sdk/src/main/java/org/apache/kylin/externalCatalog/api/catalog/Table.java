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
package org.apache.kylin.externalCatalog.api.catalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.externalCatalog.api.annotation.InterfaceStability.Evolving;

@Evolving
public class Table {

    public enum Format {
        JSON, CSV, PARQUET, ORC
    }

    /**
     * Typesafe enum for types of tables described by the metastore.
     */
    public enum Type {
        EXTERNAL_TABLE, VIEW
    }

    public static class StorageDescriptor {
        String path;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }

    private List<FieldSchema> partitionColumnNames; // required
    private final String tableName; // required
    private final String dbName; // required
    private String owner; // required
    private int createTime; // required
    private int lastAccessTime; // required
    private String format;
    private final StorageDescriptor sd; // required
    private List<FieldSchema> fields; // required fields can not contain partition col
    private final Map<String, String> parameters; // required
    private String viewText;
    private String tableType;

    public Table(String tableName, String dbName) {
        this.tableName = tableName;
        this.dbName = dbName;
        this.sd = new StorageDescriptor();
        this.parameters = new HashMap<>();
    }

    public List<FieldSchema> getPartitionColumnNames() {
        return partitionColumnNames;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public int getCreateTime() {
        return createTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }

    public int getLastAccessTime() {
        return lastAccessTime;
    }

    public void setPartitionColumnNames(List<FieldSchema> partitionColumnNames) {
        this.partitionColumnNames = partitionColumnNames;
    }

    public void setLastAccessTime(int lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format.name();
    }

    public List<FieldSchema> getFields() {
        return fields;
    }

    public void setFields(List<FieldSchema> fields) {
        this.fields = fields;
    }

    public StorageDescriptor getSd() {
        return sd;
    }

    public void setProperty(String name, String value) {
        parameters.put(name, value);
    }

    public String getProperty(String name) {
        return parameters.get(name);
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public String getViewText() {
        return viewText;
    }

    public void setViewText(String viewText) {
        this.viewText = viewText;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(Type tableType) {
        this.tableType = tableType.name();
    }
}
