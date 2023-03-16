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

package io.kyligence.kap.secondstorage.ddl;

import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_NOT_SUPPORT_TYPE;

import java.util.Set;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.datatype.DataType;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import io.kyligence.kap.secondstorage.enums.SkippingIndexType;

public class SkippingIndexChooser {
    private static final Set<String> MINMAX_TYPE = ImmutableSet.of(DataType.TINY_INT, DataType.SMALL_INT,
            DataType.INTEGER, DataType.INT, DataType.BIGINT, DataType.TIMESTAMP, DataType.DATE);

    private static final Set<String> BLOOM_FILTER_TYPE = ImmutableSet.of(DataType.VARCHAR, DataType.CHAR,
            DataType.STRING);

    private static final Set<String> SET_TYPE = ImmutableSet.of(DataType.BOOLEAN);

    private SkippingIndexChooser() {
        // can't new
    }

    public static SkippingIndexType getSkippingIndexType(DataType dt) {
        if (isMinMax(dt)) {
            return SkippingIndexType.MINMAX;
        } else if (isSet(dt)) {
            return SkippingIndexType.SET;
        } else if (isBloomFilter(dt)) {
            return SkippingIndexType.BLOOM_FILTER;
        } else {
            throw new KylinException(SECOND_STORAGE_NOT_SUPPORT_TYPE,
                    MsgPicker.getMsg().getSecondStorageNotSupportType(dt.getName()));
        }
    }

    private static boolean isMinMax(DataType dataType) {
        return MINMAX_TYPE.contains(dataType.getName());
    }

    private static boolean isSet(DataType dataType) {
        return SET_TYPE.contains(dataType.getName());
    }

    private static boolean isBloomFilter(DataType dataType) {
        return BLOOM_FILTER_TYPE.contains(dataType.getName());
    }
}
