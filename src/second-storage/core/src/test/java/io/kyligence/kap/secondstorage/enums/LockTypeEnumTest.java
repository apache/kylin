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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LockTypeEnumTest {
    private final List<String> lockTypes = Arrays.stream(LockTypeEnum.values()).map(x -> x.name())
            .collect(Collectors.toList());

    @Test
    void testCheckLocks() {
        List<String> requestLocks = Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name());
        List<String> existLocks = Arrays.asList(LockTypeEnum.ALL.name());
        List<String> existLocks2 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> requestLocks2 = Arrays.asList(LockTypeEnum.QUERY.name());

        LockTypeEnum.checkLocks(null, null);
        LockTypeEnum.checkLocks(requestLocks, null);
        LockTypeEnum.checkLocks(null, existLocks);

        Exception exception = Assertions.assertThrows(KylinException.class,
                () -> LockTypeEnum.checkLocks(requestLocks, existLocks));
        Assertions.assertEquals(exception.getMessage(), MsgPicker.getMsg().getProjectLocked());
        MsgPicker.setMsg("cn");
        Exception exception2 = Assertions.assertThrows(KylinException.class,
                () -> LockTypeEnum.checkLocks(requestLocks, existLocks2));
        Assertions.assertEquals(exception2.getMessage(), MsgPicker.getMsg().getProjectLocked());
        MsgPicker.setMsg("en");

        Exception exception3 = Assertions.assertThrows(KylinException.class,
                () -> LockTypeEnum.checkLocks(existLocks2, requestLocks));
        Assertions.assertEquals(exception3.getMessage(), MsgPicker.getMsg().getProjectLocked());

        LockTypeEnum.checkLocks(existLocks2, requestLocks2);
    }

    @Test
    void testCheckSuccess() {
        LockTypeEnum.check(lockTypes);
    }

    @Test
    void testCheckNull() {
        Assertions.assertThrows(KylinException.class, () -> LockTypeEnum.check(null),
                REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("lockType"));
    }

    @Test
    void testCheckError() {
        List<String> lockTypesError = new ArrayList<>(lockTypes);
        lockTypesError.add(UUID.randomUUID().toString());
        Assertions.assertThrows(KylinException.class, () -> LockTypeEnum.check(lockTypesError),
                PARAMETER_INVALID_SUPPORT_LIST.getMsg("lockType", "QUERY, LOAD, ALL"));
    }

    @Test
    void testParse() {
        Assertions.assertNull(LockTypeEnum.parse(null));
        Assertions.assertNull(LockTypeEnum.parse(UUID.randomUUID().toString()));
        Assertions.assertEquals(LockTypeEnum.LOAD, LockTypeEnum.parse(LockTypeEnum.LOAD.name()));
    }

    @Test
    void testSubtract() {
        List<String> list1 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> list2 = new ArrayList<>();
        List<String> resultList1 = LockTypeEnum.subtract(list1, list2);
        Assertions.assertIterableEquals(resultList1, list1);

        List<String> resultList2 = LockTypeEnum.subtract(list2, list1);
        Assertions.assertIterableEquals(resultList2, list2);

        List<String> list3 = Arrays.asList(LockTypeEnum.ALL.name());
        List<String> resultList3 = LockTypeEnum.subtract(list1, list3);
        Assertions.assertIterableEquals(resultList3, new ArrayList<>());

        List<String> resultList4 = LockTypeEnum.subtract(list2, list3);
        Assertions.assertIterableEquals(resultList4, new ArrayList<>());

        List<String> list5 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> list6 = Arrays.asList(LockTypeEnum.QUERY.name());

        List<String> resultList5 = LockTypeEnum.subtract(list5, list6);
        Assertions.assertIterableEquals(resultList5, Arrays.asList(LockTypeEnum.LOAD.name()));

        List<String> resultList6 = LockTypeEnum.subtract(list6, list5);
        Assertions.assertIterableEquals(resultList6, Arrays.asList(LockTypeEnum.QUERY.name()));
    }

    @Test
    void testMerge() {
        List<String> list1 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> list2 = new ArrayList<>();
        List<String> resultList1 = LockTypeEnum.merge(list1, list2);
        Assertions.assertIterableEquals(resultList1, list1);

        List<String> resultList2 = LockTypeEnum.merge(list2, list1);
        Assertions.assertIterableEquals(resultList2, list1);

        List<String> list3 = Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.ALL.name());
        List<String> list4 = Arrays.asList(LockTypeEnum.LOAD.name());

        List<String> resultList3 = LockTypeEnum.merge(list3, list4);
        Assertions.assertIterableEquals(resultList3, Arrays.asList(LockTypeEnum.ALL.name()));

        List<String> resultList4 = LockTypeEnum.merge(list4, list3);
        Assertions.assertIterableEquals(resultList4, Arrays.asList(LockTypeEnum.ALL.name()));

        List<String> list5 = Arrays.asList(LockTypeEnum.LOAD.name());
        List<String> list6 = Arrays.asList(LockTypeEnum.QUERY.name());

        List<String> resultList5 = LockTypeEnum.merge(list5, list6);
        Assertions.assertIterableEquals(resultList5,
                Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));

        List<String> resultList6 = LockTypeEnum.merge(list6, list5);
        Assertions.assertIterableEquals(resultList6,
                Arrays.asList(LockTypeEnum.QUERY.name(), LockTypeEnum.LOAD.name()));
    }
}
