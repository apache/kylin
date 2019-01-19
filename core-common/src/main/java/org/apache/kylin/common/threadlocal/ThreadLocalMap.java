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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The internal data structure that stores the threadLocal variables for Netty and all {@link Thread}s.
 * Note that this class is for internal use only. Use {@link Thread}
 * unless you know what you are doing.
 */
public final class ThreadLocalMap {

    private Object[] indexedVariables;

    private static ThreadLocal<ThreadLocalMap> slowThreadLocalMap = new ThreadLocal<ThreadLocalMap>();

    private static final AtomicInteger nextIndex = new AtomicInteger();

    public static final Object UNSET = new Object();

    public static ThreadLocalMap getIfSet() {
        java.lang.Thread thread = java.lang.Thread.currentThread();
        if (thread instanceof Thread) {
            return ((Thread) thread).threadLocalMap();
        }
        return slowThreadLocalMap.get();
    }

    public static ThreadLocalMap get() {
        java.lang.Thread thread = java.lang.Thread.currentThread();
        if (thread instanceof Thread) {
            return fastGet((Thread) thread);
        }
        return slowGet();
    }

    public static void remove() {
        java.lang.Thread thread = java.lang.Thread.currentThread();
        if (thread instanceof Thread) {
            ((Thread) thread).setThreadLocalMap(null);
        } else {
            slowThreadLocalMap.remove();
        }
    }

    public static void destroy() {
        slowThreadLocalMap = null;
    }

    public static int nextVariableIndex() {
        int index = nextIndex.getAndIncrement();
        if (index < 0) {
            nextIndex.decrementAndGet();
            throw new IllegalStateException("Too many thread-local indexed variables");
        }
        return index;
    }

    public static int lastVariableIndex() {
        return nextIndex.get() - 1;
    }

    private ThreadLocalMap() {
        indexedVariables = newIndexedVariableTable();
    }

    public Object indexedVariable(int index) {
        Object[] lookup = indexedVariables;
        return index < lookup.length ? lookup[index] : UNSET;
    }

    /**
     * @return {@code true} if and only if a new thread-local variable has been created
     */
    public boolean setIndexedVariable(int index, Object value) {
        Object[] lookup = indexedVariables;
        if (index < lookup.length) {
            Object oldValue = lookup[index];
            lookup[index] = value;
            return oldValue == UNSET;
        } else {
            expandIndexedVariableTableAndSet(index, value);
            return true;
        }
    }

    public Object removeIndexedVariable(int index) {
        Object[] lookup = indexedVariables;
        if (index < lookup.length) {
            Object v = lookup[index];
            lookup[index] = UNSET;
            return v;
        } else {
            return UNSET;
        }
    }

    public int size() {
        int count = 0;
        for (Object o : indexedVariables) {
            if (o != UNSET) {
                ++count;
            }
        }

        //the fist element in `indexedVariables` is a set to keep all the ThreadLocal to remove
        //look at method `addToVariablesToRemove`
        return count - 1;
    }

    private static Object[] newIndexedVariableTable() {
        Object[] array = new Object[32];
        Arrays.fill(array, UNSET);
        return array;
    }

    private static ThreadLocalMap fastGet(Thread thread) {
        ThreadLocalMap threadLocalMap = thread.threadLocalMap();
        if (threadLocalMap == null) {
            thread.setThreadLocalMap(threadLocalMap = new ThreadLocalMap());
        }
        return threadLocalMap;
    }

    private static ThreadLocalMap slowGet() {
        ThreadLocal<ThreadLocalMap> slowThreadLocalMap = ThreadLocalMap.slowThreadLocalMap;
        ThreadLocalMap ret = slowThreadLocalMap.get();
        if (ret == null) {
            ret = new ThreadLocalMap();
            slowThreadLocalMap.set(ret);
        }
        return ret;
    }

    private void expandIndexedVariableTableAndSet(int index, Object value) {
        Object[] oldArray = indexedVariables;
        final int oldCapacity = oldArray.length;
        int newCapacity = index;
        newCapacity |= newCapacity >>> 1;
        newCapacity |= newCapacity >>> 2;
        newCapacity |= newCapacity >>> 4;
        newCapacity |= newCapacity >>> 8;
        newCapacity |= newCapacity >>> 16;
        newCapacity++;

        Object[] newArray = Arrays.copyOf(oldArray, newCapacity);
        Arrays.fill(newArray, oldCapacity, newArray.length, UNSET);
        newArray[index] = value;
        indexedVariables = newArray;
    }
}