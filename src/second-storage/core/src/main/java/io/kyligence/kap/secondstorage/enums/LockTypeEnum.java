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

import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_LOCKING;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;

public enum LockTypeEnum {
    QUERY, LOAD, ALL;

    public static List<String> subtract(List<String> existLockTypes, List<String> newLockTypes) {
        if (CollectionUtils.isEmpty(existLockTypes)) {
            return new ArrayList<>();
        } else if (CollectionUtils.isEmpty(newLockTypes)) {
            return new ArrayList<>(existLockTypes);
        } else if (newLockTypes.contains(LockTypeEnum.ALL.name())) {
            return new ArrayList<>();
        } else {
            List<String> resultList = new ArrayList<>();
            existLockTypes.stream().forEach(x -> {
                if (!newLockTypes.contains(x)) {
                    resultList.add(x);
                }
            });
            return resultList;
        }
    }

    public static List<String> merge(List<String> existLockTypes, List<String> newLockTypes) {
        if (CollectionUtils.isEmpty(existLockTypes)) {
            return new ArrayList<>(newLockTypes);
        } else if (CollectionUtils.isEmpty(newLockTypes)) {
            return new ArrayList<>(existLockTypes);
        } else if (existLockTypes.contains(LockTypeEnum.ALL.name())
                || newLockTypes.contains(LockTypeEnum.ALL.name())) {
            return Arrays.asList(LockTypeEnum.ALL.name());
        } else {
            List<String> resultList = new ArrayList<>(existLockTypes);
            newLockTypes.stream().forEach(x -> {
                if (!resultList.contains(x)) {
                    resultList.add(x);
                }
            });
            return resultList;
        }
    }

    public static LockTypeEnum parse(String value) {
        if (value == null) {
            return null;
        }
        for (LockTypeEnum lockTypeEnum : LockTypeEnum.values()) {
            if (lockTypeEnum.name().equals(value)) {
                return lockTypeEnum;
            }
        }
        return null;
    }

    public static boolean locked(String requestLock, List<String> existLocks) {
        if (requestLock == null) return false;
        return locked(Arrays.asList(requestLock), existLocks);
    }

    public static boolean locked(List<String> requestLocks, List<String> existLocks) {
        if (CollectionUtils.isEmpty(requestLocks) || CollectionUtils.isEmpty(existLocks)) {
            return false;
        }

        Set<String> requestLockSet = new HashSet<>(requestLocks);

        if (requestLockSet.contains(LockTypeEnum.ALL.name()) && !existLocks.isEmpty()) {
            return true;
        }

        Set<String> existLockSet = new HashSet<>(existLocks);

        if (existLockSet.contains(LockTypeEnum.ALL.name()) || CollectionUtils.intersection(requestLockSet, existLockSet).size() > 0) {
            return true;
        }

        return false;
    }

    public static void checkLock(String requestLock, List<String> existLocks) {
        if (requestLock == null) return;
        if (locked(Arrays.asList(requestLock), existLocks)) {
            throw new KylinException(SECOND_STORAGE_PROJECT_LOCKING, String.format(Locale.ROOT, MsgPicker.getMsg().getProjectLocked()));
        }
    }

    public static void checkLocks(List<String> requestLocks, List<String> existLocks) {
        if (locked(requestLocks, existLocks)) {
            throw new KylinException(SECOND_STORAGE_PROJECT_LOCKING, String.format(Locale.ROOT, MsgPicker.getMsg().getProjectLocked()));
        }
    }

    public static void check(List<String> lockTypes) {
        if (lockTypes == null || CollectionUtils.isEmpty(lockTypes)) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, "lockType");
        }
        lockTypes.stream().forEach(x -> {
            LockTypeEnum typeEnum = LockTypeEnum.parse(x);
            if (typeEnum == null) {
                throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "lockType", "QUERY, LOAD, ALL");
            }
        });
    }
}
