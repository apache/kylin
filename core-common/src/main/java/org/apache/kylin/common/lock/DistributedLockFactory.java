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

package org.apache.kylin.common.lock;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;

public abstract class DistributedLockFactory {

    abstract public DistributedLock lockForClient(String client);

    public DistributedLock lockForCurrentThread() {
        return lockForClient(threadProcessAndHost());
    }

    public DistributedLock lockForCurrentProcess() {
        return lockForClient(processAndHost());
    }

    private static String threadProcessAndHost() {
        return Thread.currentThread().getId() + "-" + processAndHost();
    }

    public static String processAndHost() {
        byte[] bytes = ManagementFactory.getRuntimeMXBean().getName().getBytes(StandardCharsets.UTF_8);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
