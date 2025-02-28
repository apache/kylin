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

package org.apache.kylin.metadata.favorite;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class QueryHistoryIdOffset {

    private int id;
    private String project;
    private String type;
    private long offset;
    private long updateTime;
    private long createTime;
    private long mvcc;

    public QueryHistoryIdOffset() {
    }

    public QueryHistoryIdOffset(long offset, OffsetType type) {
        this.offset = offset;
        this.type = type.name;
    }

    public enum OffsetType {
        META("meta"), ACCELERATE("accelerate");

        private final String name;

        OffsetType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
