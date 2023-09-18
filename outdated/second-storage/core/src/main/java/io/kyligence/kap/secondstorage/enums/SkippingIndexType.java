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

package io.kyligence.kap.secondstorage.enums;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;

public enum SkippingIndexType {
    BLOOM_FILTER {
        private static final String FUNCTION_BLOOM_FILTER = "bloom_filter(%s)";

        @Override
        public String toSql(KylinConfig modelConfig) {
            return String.format(Locale.ROOT, FUNCTION_BLOOM_FILTER,
                    modelConfig.getSecondStorageSkippingIndexBloomFilter());
        }
    },

    SET {
        private static final String FUNCTION_SET = "set(%d)";

        @Override
        public String toSql(KylinConfig modelConfig) {
            return String.format(Locale.ROOT, FUNCTION_SET, modelConfig.getSecondStorageSkippingIndexSet());
        }
    },

    MINMAX {
        private static final String FUNCTION_MINMAX = "minmax()";

        @Override
        public String toSql(KylinConfig modelConfig) {
            return FUNCTION_MINMAX;
        }
    };

    public abstract String toSql(KylinConfig modelConfig);
}
