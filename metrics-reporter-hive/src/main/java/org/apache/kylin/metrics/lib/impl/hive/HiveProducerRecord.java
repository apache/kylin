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

    public HiveProducerRecord(String dbName, String tableName, Map<String, String> partitionKVs, List<Object> value) {
        this.key = new RecordKey(dbName, tableName, partitionKVs);
        this.value = value;
    }

    public HiveProducerRecord(String tableName, Map<String, String> partitionKVs, List<Object> value) {
        this.key = new RecordKey(tableName, partitionKVs);
        this.value = value;
    }

    public HiveProducerRecord(String dbName, String tableName, List<Object> value) {
        this.key = new RecordKey(dbName, tableName);
        this.value = value;
    }

    public HiveProducerRecord(String tableName, List<Object> value) {
        this.key = new RecordKey(tableName);
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

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof HiveProducerRecord)) {
            return false;
        } else {
            HiveProducerRecord that = (HiveProducerRecord) o;
            if (this.key != null) {
                if (!this.key.equals(that.key)) {
                    return false;
                }
            } else if (that.key != null) {
                return false;
            }
            if (this.value != null) {
                if (!this.value.equals(that.value)) {
                    return false;
                }
            } else if (that.value != null) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        int result = this.key != null ? this.key.hashCode() : 0;
        result = 31 * result + (this.value != null ? this.value.hashCode() : 0);
        return result;
    }

    /**
     * Use to organize metrics message
     */
    public class RecordKey {
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

        public RecordKey(String tableName, Map<String, String> partitionKVs) {
            this(null, tableName, partitionKVs);
        }

        public RecordKey(String dbName, String tableName) {
            this(dbName, tableName, null);
        }

        public RecordKey(String tableName) {
            this(null, tableName, null);
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
            return "RecordKey(database=" + this.dbName + ", table=" + this.tableName + ", partition=" + partitionKVs + ")";
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (!(o instanceof RecordKey)) {
                return false;
            } else {
                RecordKey that = (RecordKey) o;
                if (this.dbName != null) {
                    if (!this.dbName.equals(that.dbName)) {
                        return false;
                    }
                } else if (that.dbName != null) {
                    return false;
                }

                if (this.tableName != null) {
                    if (!this.tableName.equals(that.tableName)) {
                        return false;
                    }
                } else if (that.tableName != null) {
                    return false;
                }

                if (this.partitionKVs != null) {
                    if (!this.partitionKVs.equals(that.partitionKVs)) {
                        return false;
                    }
                } else if (that.partitionKVs != null) {
                    return false;
                }
            }
            return true;
        }

        public int hashCode() {
            int result = this.dbName != null ? this.dbName.hashCode() : 0;
            result = 31 * result + (this.tableName != null ? this.tableName.hashCode() : 0);
            result = 31 * result + (this.partitionKVs != null ? this.partitionKVs.hashCode() : 0);
            return result;
        }
    }
}
