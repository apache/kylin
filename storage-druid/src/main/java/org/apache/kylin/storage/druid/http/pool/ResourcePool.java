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

package org.apache.kylin.storage.druid.http.pool;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResourcePool<K, V> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ResourcePool.class);
    private final LoadingCache<K, ImmediateCreationResourceHolder<K, V>> pool;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ResourcePool(
            final ResourceFactory<K, V> factory,
            final int maxPerKey
    ) {
        this.pool = CacheBuilder.newBuilder().build(
                new CacheLoader<K, ImmediateCreationResourceHolder<K, V>>() {
                    @Override
                    public ImmediateCreationResourceHolder<K, V> load(K input) throws Exception {
                        return new ImmediateCreationResourceHolder<>(
                                maxPerKey,
                                input,
                                factory
                        );
                    }
                }
        );
    }

    public ResourceContainer<V> take(final K key) {
        if (closed.get()) {
            log.error(String.format(Locale.ROOT, "take(%s) called even though I'm closed.", key));
            return null;
        }

        final ImmediateCreationResourceHolder<K, V> holder;
        try {
            holder = pool.get(key);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
        final V value = holder.get();

        return new ResourceContainer<V>() {
            private final AtomicBoolean returned = new AtomicBoolean(false);

            @Override
            public V get() {
                Preconditions.checkState(!returned.get(), "Resource for key[%s] has been returned, cannot get().", key);
                return value;
            }

            @Override
            public void returnResource() {
                if (returned.getAndSet(true)) {
                    log.warn(String.format(Locale.ROOT, "Resource at key[%s] was returned multiple times?", key));
                } else {
                    holder.giveBack(value);
                }
            }

            @Override
            protected void finalize() throws Throwable {
                if (!returned.get()) {
                    log.warn(
                            String.format(Locale.ROOT,
                                    "Resource[%s] at key[%s] was not returned before Container was finalized, potential resource leak.",
                                    value,
                                    key
                            )
                    );
                    returnResource();
                }
                super.finalize();
            }
        };
    }

    public void close() {
        closed.set(true);
        final Map<K, ImmediateCreationResourceHolder<K, V>> mapView = pool.asMap();
        for (K k : ImmutableSet.copyOf(mapView.keySet())) {
            mapView.remove(k).close();
        }
    }

    private static class ImmediateCreationResourceHolder<K, V> {
        private final int maxSize;
        private final K key;
        private final ResourceFactory<K, V> factory;
        private final LinkedList<V> objectList;
        private int deficit = 0;
        private boolean closed = false;

        private ImmediateCreationResourceHolder(
                int maxSize,
                K key,
                ResourceFactory<K, V> factory
        ) {
            this.maxSize = maxSize;
            this.key = key;
            this.factory = factory;

            this.objectList = new LinkedList<V>();
            for (int i = 0; i < maxSize; ++i) {
                objectList.addLast(Preconditions.checkNotNull(factory.generate(key), "factory.generate(key)"));
            }
        }

        V get() {
            // objectList can't have nulls, so we'll use a null to signal that we need to create a new resource.
            final V poolVal;
            synchronized (this) {
                while (!closed && objectList.size() == 0 && deficit == 0) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        return null;
                    }
                }

                if (closed) {
                    log.info(String.format(Locale.ROOT, "get() called even though I'm closed. key[%s]", key));
                    return null;
                } else if (!objectList.isEmpty()) {
                    poolVal = objectList.removeFirst();
                } else if (deficit > 0) {
                    deficit--;
                    poolVal = null;
                } else {
                    throw new IllegalStateException("WTF?! No objects left, and no object deficit. This is probably a bug.");
                }
            }

            // At this point, we must either return a valid resource or increment "deficit".
            final V retVal;
            try {
                if (poolVal != null && factory.isGood(poolVal)) {
                    retVal = poolVal;
                } else {
                    if (poolVal != null) {
                        factory.close(poolVal);
                    }
                    retVal = factory.generate(key);
                }
            } catch (Throwable e) {
                synchronized (this) {
                    deficit++;
                    this.notifyAll();
                }
                throw Throwables.propagate(e);
            }

            return retVal;
        }

        void giveBack(V object) {
            Preconditions.checkNotNull(object, "object");

            synchronized (this) {
                if (closed) {
                    log.info(String.format(Locale.ROOT, "giveBack called after being closed. key[%s]", key));
                    factory.close(object);
                    return;
                }

                if (objectList.size() >= maxSize) {
                    if (objectList.contains(object)) {
                        log.warn(
                                String.format(
                                        Locale.ROOT,
                                        "Returning object[%s] at key[%s] that has already been returned!? Skipping",
                                        object,
                                        key
                                ),
                                new Exception("Exception for stacktrace")
                        );
                    } else {
                        log.warn(
                                String.format(
                                        Locale.ROOT,
                                        "Returning object[%s] at key[%s] even though we already have all that we can hold[%s]!? Skipping",
                                        object,
                                        key,
                                        objectList
                                ),
                                new Exception("Exception for stacktrace")
                        );
                    }
                    return;
                }

                objectList.addLast(object);
                this.notifyAll();
            }
        }

        void close() {
            synchronized (this) {
                closed = true;
                while (!objectList.isEmpty()) {
                    factory.close(objectList.removeFirst());
                }
                this.notifyAll();
            }
        }
    }
}
