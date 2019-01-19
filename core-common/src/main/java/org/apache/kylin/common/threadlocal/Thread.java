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
package org.apache.kylin.common.threadlocal;

/**
 * Thread
 */
public class Thread extends java.lang.Thread {

    private ThreadLocalMap threadLocalMap;

    public Thread() {
    }

    public Thread(Runnable target) {
        super(target);
    }

    public Thread(ThreadGroup group, Runnable target) {
        super(group, target);
    }

    public Thread(String name) {
        super(name);
    }

    public Thread(ThreadGroup group, String name) {
        super(group, name);
    }

    public Thread(Runnable target, String name) {
        super(target, name);
    }

    public Thread(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
    }

    public Thread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, target, name, stackSize);
    }

    /**
     * Returns the internal data structure that keeps the threadLocal variables bound to this thread.
     * Note that this method is for internal use only, and thus is subject to change at any time.
     */
    public final ThreadLocalMap threadLocalMap() {
        return threadLocalMap;
    }

    /**
     * Sets the internal data structure that keeps the threadLocal variables bound to this thread.
     * Note that this method is for internal use only, and thus is subject to change at any time.
     */
    public final void setThreadLocalMap(ThreadLocalMap threadLocalMap) {
        this.threadLocalMap = threadLocalMap;
    }
}