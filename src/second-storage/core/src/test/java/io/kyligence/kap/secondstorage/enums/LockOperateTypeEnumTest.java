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

import static io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum.LOCK;
import static io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum.check;
import static io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum.parse;
import static io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum.values;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.KylinException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LockOperateTypeEnumTest {
    private final List<String> lockOperateTypes = Arrays.stream(values()).map(Enum::name).collect(Collectors.toList());

    @Test
    void testCheckSuccess() {
        lockOperateTypes.forEach(LockOperateTypeEnum::check);
    }

    @Test
    void testCheckError() {
        String lockOperateType = UUID.randomUUID().toString();
        Assertions.assertThrows(KylinException.class, () -> check(lockOperateType),
                REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("lockOperateType"));
    }

    @Test
    void testParse() {
        Assertions.assertNull(parse(null));
        Assertions.assertNull(parse(UUID.randomUUID().toString()));
        Assertions.assertEquals(LOCK, parse(LOCK.name()));
    }
}
