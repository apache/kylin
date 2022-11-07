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

package org.apache.kylin.rest.security;

import java.util.Map;

import org.apache.kylin.metadata.user.ManagedUser;

import com.google.common.collect.Maps;

public class UserLockRuleUtil {

    private static Map<Integer, Long> lockDurationRules = Maps.newHashMap();

    static {
        // wrong time => lock duration (ms)
        lockDurationRules.put(0, 0L);
        lockDurationRules.put(1, 0L);
        lockDurationRules.put(2, 0L);
        lockDurationRules.put(3, 30 * 1000L); // 30s
        lockDurationRules.put(4, 60 * 1000L); // 1min
        lockDurationRules.put(5, 5 * 60 * 1000L); // 5min
        lockDurationRules.put(6, 10 * 60 * 1000L); // 10 min
        lockDurationRules.put(7, 30 * 60 * 1000L); // 30 min
        lockDurationRules.put(8, 24 * 3600 * 1000L); // 1d
        lockDurationRules.put(9, 72 * 3600 * 1000L); // 3d
        lockDurationRules.put(10, Long.MAX_VALUE); // lock permanently
    }

    public static long getLockDuration(int wrongTime) {
        if (wrongTime >= 0 && wrongTime <= 10) {
            return lockDurationRules.get(wrongTime);
        }
        return lockDurationRules.get(10);
    }

    public static long getLockDurationSeconds(int wrongTime) {
        long lockDurationMs = getLockDuration(wrongTime);
        return lockDurationMs / 1000;
    }

    public static long getLockDurationSeconds(ManagedUser managedUser) {
        return getLockDurationSeconds(managedUser.getWrongTime());
    }

    public static boolean isLockedPermanently(ManagedUser managedUser) {
        return Long.MAX_VALUE == lockDurationRules.get(managedUser.getWrongTime());
    }

    public static boolean isLockDurationEnded(ManagedUser managedUser, long duration) {
        return duration >= getLockDuration(managedUser.getWrongTime());
    }

    public static long getLockLeftSeconds(ManagedUser managedUser, long duration) {
        long lockDuration = getLockDuration(managedUser.getWrongTime());
        if (Long.MAX_VALUE == lockDuration) {
            return Long.MAX_VALUE;
        }
        long leftSeconds = (lockDuration - duration) / 1000;
        return leftSeconds <= 0 ? 1 : leftSeconds;
    }
}
