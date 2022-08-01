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

package org.apache.kylin.common.lock;

import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.concurrent.locks.Lock;

public abstract class DistributedLockFactory {

    public abstract Lock getLockForClient(String client, String key);

    public abstract void initialize();

    public Lock getLockForCurrentThread(String key) {
        return getLockForClient(threadProcessAndHost(), key);
    }

    public Lock lockForCurrentProcess(String key) {
        return getLockForClient(processAndHost(), key);
    }

    private static String threadProcessAndHost() {
        return Thread.currentThread().getId() + "-" + processAndHost();
    }

    private static String processAndHost() {
        byte[] bytes = ManagementFactory.getRuntimeMXBean().getName().getBytes(Charset.defaultCharset());
        return new String(bytes, Charset.defaultCharset());
    }
}
