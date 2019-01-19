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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * ThreadLocal
 * A special variant of {@link java.lang.ThreadLocal} that yields higher access performance when accessed from a
 * {@link Thread}.
 * <p></p>
 * Internally, a {@link Thread} uses a constant index in an array, instead of using hash code and hash table,
 * to look for a variable.  Although seemingly very subtle, it yields slight performance advantage over using a hash
 * table, and it is useful when accessed frequently.
 * <p></p>
 * This design is learning from {@see io.netty.util.concurrent.FastThreadLocal} which is in Netty.
 */
public class ThreadLocal<V> {

    private static final int variablesToRemoveIndex = ThreadLocalMap.nextVariableIndex();

    private final int index;

    public ThreadLocal() {
        index = ThreadLocalMap.nextVariableIndex();
    }

    /**
     * Removes all {@link ThreadLocal} variables bound to the current thread.  This operation is useful when you
     * are in a container environment, and you don't want to leave the thread local variables in the threads you do not
     * manage.
     */
    @SuppressWarnings("unchecked")
    public static void removeAll() {
        ThreadLocalMap threadLocalMap = ThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return;
        }

        try {
            Object v = threadLocalMap.indexedVariable(variablesToRemoveIndex);
            if (v != null && v != ThreadLocalMap.UNSET) {
                Set<ThreadLocal<?>> variablesToRemove = (Set<ThreadLocal<?>>) v;
                ThreadLocal<?>[] variablesToRemoveArray =
                        variablesToRemove.toArray(new ThreadLocal[variablesToRemove.size()]);
                for (ThreadLocal<?> tlv : variablesToRemoveArray) {
                    tlv.remove(threadLocalMap);
                }
            }
        } finally {
            ThreadLocalMap.remove();
        }
    }

    /**
     * Returns the number of thread local variables bound to the current thread.
     */
    public static int size() {
        ThreadLocalMap threadLocalMap = ThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return 0;
        } else {
            return threadLocalMap.size();
        }
    }

    public static void destroy() {
        ThreadLocalMap.destroy();
    }

    @SuppressWarnings("unchecked")
    private static void addToVariablesToRemove(ThreadLocalMap threadLocalMap, ThreadLocal<?> variable) {
        Object v = threadLocalMap.indexedVariable(variablesToRemoveIndex);
        Set<ThreadLocal<?>> variablesToRemove;
        if (v == ThreadLocalMap.UNSET || v == null) {
            variablesToRemove = Collections.newSetFromMap(new IdentityHashMap<ThreadLocal<?>, Boolean>());
            threadLocalMap.setIndexedVariable(variablesToRemoveIndex, variablesToRemove);
        } else {
            variablesToRemove = (Set<ThreadLocal<?>>) v;
        }

        variablesToRemove.add(variable);
    }

    @SuppressWarnings("unchecked")
    private static void removeFromVariablesToRemove(ThreadLocalMap threadLocalMap, ThreadLocal<?> variable) {

        Object v = threadLocalMap.indexedVariable(variablesToRemoveIndex);

        if (v == ThreadLocalMap.UNSET || v == null) {
            return;
        }

        Set<ThreadLocal<?>> variablesToRemove = (Set<ThreadLocal<?>>) v;
        variablesToRemove.remove(variable);
    }

    /**
     * Returns the current value for the current thread
     */
    @SuppressWarnings("unchecked")
    public final V get() {
        ThreadLocalMap threadLocalMap = ThreadLocalMap.get();
        Object v = threadLocalMap.indexedVariable(index);
        if (v != ThreadLocalMap.UNSET) {
            return (V) v;
        }

        return initialize(threadLocalMap);
    }

    private V initialize(ThreadLocalMap threadLocalMap) {
        V v = null;
        try {
            v = initialValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        threadLocalMap.setIndexedVariable(index, v);
        addToVariablesToRemove(threadLocalMap, this);
        return v;
    }

    /**
     * Sets the value for the current thread.
     */
    public final void set(V value) {
        if (value == null || value == ThreadLocalMap.UNSET) {
            remove();
        } else {
            ThreadLocalMap threadLocalMap = ThreadLocalMap.get();
            if (threadLocalMap.setIndexedVariable(index, value)) {
                addToVariablesToRemove(threadLocalMap, this);
            }
        }
    }

    /**
     * Sets the value to uninitialized; a proceeding call to get() will trigger a call to initialValue().
     */
    @SuppressWarnings("unchecked")
    public final void remove() {
        remove(ThreadLocalMap.getIfSet());
    }

    /**
     * Sets the value to uninitialized for the specified thread local map;
     * a proceeding call to get() will trigger a call to initialValue().
     * The specified thread local map must be for the current thread.
     */
    @SuppressWarnings("unchecked")
    public final void remove(ThreadLocalMap threadLocalMap) {
        if (threadLocalMap == null) {
            return;
        }

        Object v = threadLocalMap.removeIndexedVariable(index);
        removeFromVariablesToRemove(threadLocalMap, this);

        if (v != ThreadLocalMap.UNSET) {
            try {
                onRemoval((V) v);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Returns the initial value for this thread-local variable.
     */
    protected V initialValue() throws Exception {
        return null;
    }

    /**
     * Invoked when this thread local variable is removed by {@link #remove()}.
     */
    protected void onRemoval(@SuppressWarnings("unused") V value) throws Exception {
    }
}