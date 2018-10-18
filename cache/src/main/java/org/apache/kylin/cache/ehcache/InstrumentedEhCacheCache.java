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

package org.apache.kylin.cache.ehcache;

import static org.apache.kylin.metrics.lib.impl.MetricsSystem.Metrics;
import static org.apache.kylin.metrics.lib.impl.MetricsSystem.name;

import java.util.concurrent.Callable;

import org.springframework.cache.Cache;
import org.springframework.cache.ehcache.EhCacheCache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.util.Assert;

import com.codahale.metrics.Gauge;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;

/**
 * {@link Cache} implementation on top of an {@link Ehcache} instance.
 *
 */
public class InstrumentedEhCacheCache implements Cache {

    private final Ehcache cache;

    /**
     * Create an {@link EhCacheCache} instance.
     * @param ehcache backing Ehcache instance
     */
    public InstrumentedEhCacheCache(Ehcache ehcache) {
        Assert.notNull(ehcache, "Ehcache must not be null");
        Status status = ehcache.getStatus();
        Assert.isTrue(Status.STATUS_ALIVE.equals(status),
                "An 'alive' Ehcache is required - current cache is " + status.toString());
        this.cache = ehcache;

        final String prefix = name(cache.getClass(), cache.getName());
        Metrics.register(name(prefix, "hits"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.getStatistics().cacheHitCount();
            }
        });

        Metrics.register(name(prefix, "in-memory-hits"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.getStatistics().localHeapHitCount();
            }
        });

        Metrics.register(name(prefix, "misses"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.getStatistics().cacheMissCount();
            }
        });

        Metrics.register(name(prefix, "in-memory-misses"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.getStatistics().localHeapMissCount();
            }
        });

        Metrics.register(name(prefix, "objects"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.getStatistics().getSize();
            }
        });

        Metrics.register(name(prefix, "in-memory-objects"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.getStatistics().getLocalHeapSize();
            }
        });

        Metrics.register(name(prefix, "mean-get-time"), new Gauge<Double>() {
            @Override
            public Double getValue() {
                return cache.getStatistics().cacheGetOperation().latency().average().value();
            }
        });

        Metrics.register(name(prefix, "mean-search-time"), new Gauge<Double>() {
            @Override
            public Double getValue() {
                return cache.getStatistics().cacheSearchOperation().latency().average().value();
            }
        });

        Metrics.register(name(prefix, "eviction-count"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.getStatistics().cacheEvictionOperation().count().value();
            }
        });

        Metrics.register(name(prefix, "writer-queue-size"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.getStatistics().getWriterQueueLength();
            }
        });
    }

    public String getName() {
        return this.cache.getName();
    }

    public Ehcache getNativeCache() {
        return this.cache;
    }

    public ValueWrapper get(Object key) {
        Element element = this.cache.get(key);
        return (element != null ? new SimpleValueWrapper(element.getObjectValue()) : null);
    }

    public void put(Object key, Object value) {
        this.cache.put(new Element(key, value));
    }

    public void evict(Object key) {
        this.cache.remove(key);
    }

    public void clear() {
        this.cache.removeAll();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Class<T> type) {
        Element element = lookup(key);
        Object value = (element != null ? element.getObjectValue() : null);
        if (value != null && type != null && !type.isInstance(value)) {
            throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + value);
        }
        return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> valueLoader) {
        Element element = lookup(key);
        if (element != null) {
            return (T) element.getObjectValue();
        } else {
            this.cache.acquireWriteLockOnKey(key);
            try {
                element = lookup(key); // One more attempt with the write lock
                if (element != null) {
                    return (T) element.getObjectValue();
                } else {
                    return loadValue(key, valueLoader);
                }
            } finally {
                this.cache.releaseWriteLockOnKey(key);
            }
        }
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        Element existingElement = this.cache.putIfAbsent(new Element(key, value));
        return (existingElement != null ? new SimpleValueWrapper(existingElement.getObjectValue()) : null);
    }

    private Element lookup(Object key) {
        return this.cache.get(key);
    }

    private <T> T loadValue(Object key, Callable<T> valueLoader) {
        T value;
        try {
            value = valueLoader.call();
        } catch (Throwable ex) {
            throw new ValueRetrievalException(key, valueLoader, ex);
        }
        put(key, value);
        return value;
    }
}