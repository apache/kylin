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

package org.apache.kylin.common.util;

import java.io.Closeable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AutoReadWriteLock {

    public interface AutoLock extends Closeable {
        // a close() that don't throw exception
        public void close();
    }

    // ============================================================================

    final private ReentrantReadWriteLock rwlock;

    public AutoReadWriteLock() {
        this(new ReentrantReadWriteLock());
    }

    public AutoReadWriteLock(ReentrantReadWriteLock rwlock) {
        this.rwlock = rwlock;
    }

    public ReentrantReadWriteLock innerLock() {
        return rwlock;
    }

    public AutoLock lockForRead() {
        rwlock.readLock().lock();
        return new AutoLock() {
            @Override
            public void close() {
                rwlock.readLock().unlock();
            }
        };
    }

    public AutoLock lockForWrite() {
        rwlock.writeLock().lock();
        return new AutoLock() {
            @Override
            public void close() {
                rwlock.writeLock().unlock();
            }
        };
    }
}
