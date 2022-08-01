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

package org.apache.kylin.job.execution;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.Getter;

public enum JobTypeEnum {
    INDEX_REFRESH(Category.BUILD), //
    INDEX_MERGE(Category.BUILD), //
    INDEX_BUILD(Category.BUILD), //
    INC_BUILD(Category.BUILD), //
    SUB_PARTITION_BUILD(Category.BUILD), // 
    SUB_PARTITION_REFRESH(Category.BUILD), //

    SNAPSHOT_BUILD(Category.SNAPSHOT), //
    SNAPSHOT_REFRESH(Category.SNAPSHOT), //

    STREAMING_MERGE(Category.STREAMING), //
    STREAMING_BUILD(Category.STREAMING), //

    ASYNC_QUERY(Category.ASYNC_QUERY), //

    EXPORT_TO_SECOND_STORAGE(Category.SECOND_STORAGE), //
    SECOND_STORAGE_MODEL_CLEAN(Category.SECOND_STORAGE), //
    SECOND_STORAGE_NODE_CLEAN(Category.SECOND_STORAGE), //
    SECOND_STORAGE_SEGMENT_CLEAN(Category.SECOND_STORAGE), //
    SECOND_STORAGE_INDEX_CLEAN(Category.SECOND_STORAGE), //

    TABLE_SAMPLING(Category.OTHER), STAGE(Category.OTHER);

    @Getter
    private final String category;

    JobTypeEnum(String category) {
        this.category = category;
    }

    public static class Category {
        public static final String BUILD = "BUILD";
        public static final String SNAPSHOT = "SNAPSHOT";
        public static final String STREAMING = "STREAMING";
        public static final String SECOND_STORAGE = "SECOND_STORAGE";
        public static final String ASYNC_QUERY = "ASYNC_QUERY";
        public static final String OTHER = "OTHER";

        private Category() {
        }
    }

    public static List<JobTypeEnum> getJobTypeByCategory(String category) {
        return Arrays.stream(JobTypeEnum.values()).filter(e -> e.getCategory().equals(category))
                .collect(Collectors.toList());
    }

    public static JobTypeEnum getEnumByName(String name) {
        for (JobTypeEnum value : JobTypeEnum.values()) {
            if (value.name().equals(name)) {
                return value;
            }
        }
        return null;
    }
}
