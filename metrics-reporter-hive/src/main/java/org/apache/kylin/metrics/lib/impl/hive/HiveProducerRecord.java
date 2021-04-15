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

package org.apache.kylin.metrics.lib.impl.hive;

import java.util.List;
import java.util.Map;

import org.apache.kylin.shaded.com.google.common.base.Strings;

public class HiveProducerRecord {

    public static final String DELIMITER = ",";

    private final RecordKey key;
    private final List<Object> value;

    public HiveProducerRecord(RecordKey key, List<Object> value) {
        this.key = key;
        this.value = value;
    }

    public RecordKey key() {
        return this.key;
    }

    public List<Object> value() {
        return this.value;
    }

    public String toString() {
        String value = this.value == null ? "null" : this.value.toString();
        return "HiveProducerRecord(key=" + this.key.toString() + ", value=" + value + ")";
    }

    public String valueToString() {
        if (this.value == null || value.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.size() - 1; i++) {
            sb.append(value.get(i) + DELIMITER);
        }
        sb.append(value.get(value.size() - 1));
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        HiveProducerRecord record = (HiveProducerRecord) o;

        if (key != null ? !key.equals(record.key) : record.key != null)
            return false;
        return value != null ? value.equals(record.value) : record.value == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    public static class KeyBuilder {
        private final String tableName;
        private String dbName;
        private Map<String, String> partitionKVs;

        public KeyBuilder(String tableName) {
            this.tableName = tableName;
        }

        public KeyBuilder setDbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public KeyBuilder setPartitionKVs(Map<String, String> partitionKVs) {
            this.partitionKVs = partitionKVs;
            return this;
        }

        public RecordKey build() {
            return new RecordKey(dbName, tableName, partitionKVs);
        }
    }

    /**
     * Use to organize metrics message
     */
    public static class RecordKey {
        public static final String DEFAULT_DB_NAME = "DEFAULT";

        private final String dbName;
        private final String tableName;
        private final Map<String, String> partitionKVs;

        public RecordKey(String dbName, String tableName, Map<String, String> partitionKVs) {
            if (Strings.isNullOrEmpty(dbName)) {
                this.dbName = DEFAULT_DB_NAME;
            } else {
                this.dbName = dbName;
            }
            this.tableName = tableName;
            this.partitionKVs = partitionKVs;
        }

        public String database() {
            return this.dbName;
        }

        public String table() {
            return this.tableName;
        }

        public Map<String, String> partition() {
            return this.partitionKVs;
        }

        public String toString() {
            String partitionKVs = this.partitionKVs == null ? "null" : this.partitionKVs.toString();
            return "RecordKey(database=" + this.dbName + ", table=" + this.tableName + ", partition=" + partitionKVs
                    + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            RecordKey recordKey = (RecordKey) o;

            if (dbName != null ? !dbName.equals(recordKey.dbName) : recordKey.dbName != null)
                return false;
            if (tableName != null ? !tableName.equals(recordKey.tableName) : recordKey.tableName != null)
                return false;
            return partitionKVs != null ? partitionKVs.equals(recordKey.partitionKVs) : recordKey.partitionKVs == null;
        }

        @Override
        public int hashCode() {
            int result = dbName != null ? dbName.hashCode() : 0;
            result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
            result = 31 * result + (partitionKVs != null ? partitionKVs.hashCode() : 0);
            return result;
        }
    }
}
