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
package org.apache.kylin.metadata.model.schema;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.val;

public enum SchemaNodeType {
    TABLE_COLUMN {
        @Override
        public String getSubject(String key) {
            return key.substring(0, key.lastIndexOf('.'));
        }

        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return key.substring(key.lastIndexOf('.') + 1);
        }
    },
    MODEL_COLUMN {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    },
    MODEL_CC, //
    MODEL_TABLE {
        @Override
        public String getSubject(String key) {
            return key;
        }

        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return key;
        }
    },
    MODEL_FACT, MODEL_DIM, //
    MODEL_PARTITION(true) {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("column");
        }
    },
    MODEL_MULTIPLE_PARTITION(true) {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return String.join(",", ((List<String>) attributes.get("columns")));
        }
    },
    MODEL_JOIN(true), MODEL_FILTER(true) {
        @Override
        public String getSubject(String key) {
            return key;
        }

        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("condition");
        }
    }, //
    MODEL_DIMENSION {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    },
    MODEL_MEASURE {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    }, //
    WHITE_LIST_INDEX {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    },
    TO_BE_DELETED_INDEX {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    },
    RULE_BASED_INDEX {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    }, //
    AGG_GROUP, INDEX_AGG_SHARD, INDEX_AGG_EXTEND_PARTITION;

    @Getter
    boolean causeModelBroken;

    SchemaNodeType() {
        this(false);
    }

    SchemaNodeType(boolean causeModelBroken) {
        this.causeModelBroken = causeModelBroken;
    }

    public SchemaNode withKey(String key) {
        return new SchemaNode(this, key);
    }

    public boolean isModelNode() {
        return this != TABLE_COLUMN && this != MODEL_TABLE;
    }

    public String getSubject(String key) {
        return key.split("/")[0];
    }

    public String getDetail(String key, Map<String, Object> attributes) {
        val words = key.split("/");
        if (words.length == 2) {
            return words[1];
        }
        return this.toString();
    }
}
