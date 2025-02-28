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

package org.apache.kylin.rest.cache.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.FailureMode;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.metrics.MetricCollector;
import net.spy.memcached.metrics.MetricType;
import net.spy.memcached.metrics.NoopMetricCollector;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.transcoders.Transcoder;

public class MemcachedConnectionFactory extends SpyObject implements ConnectionFactory {
    private ConnectionFactory underlying;

    public MemcachedConnectionFactory(ConnectionFactory underlying) {
        this.underlying = underlying;
    }

    @Override
    public MetricType enableMetrics() {
        //Turn off metric collection by default.
        return DefaultConnectionFactory.DEFAULT_METRIC_TYPE;
    }

    @Override
    public MetricCollector getMetricCollector() {
        return new NoopMetricCollector();
    }

    @Override
    public MemcachedConnection createConnection(List<InetSocketAddress> addrs) throws IOException {
        return underlying.createConnection(addrs);
    }

    @Override
    public MemcachedNode createMemcachedNode(SocketAddress sa, SocketChannel c, int bufSize) {
        return underlying.createMemcachedNode(sa, c, bufSize);
    }

    @Override
    public BlockingQueue<Operation> createOperationQueue() {
        return underlying.createOperationQueue();
    }

    @Override
    public BlockingQueue<Operation> createReadOperationQueue() {
        return underlying.createReadOperationQueue();
    }

    @Override
    public BlockingQueue<Operation> createWriteOperationQueue() {
        return underlying.createWriteOperationQueue();
    }

    @Override
    public long getOpQueueMaxBlockTime() {
        return underlying.getOpQueueMaxBlockTime();
    }

    @Override
    public ExecutorService getListenerExecutorService() {
        return underlying.getListenerExecutorService();
    }

    @Override
    public boolean isDefaultExecutorService() {
        return underlying.isDefaultExecutorService();
    }

    @Override
    public NodeLocator createLocator(List<MemcachedNode> nodes) {
        return underlying.createLocator(nodes);
    }

    @Override
    public OperationFactory getOperationFactory() {
        return underlying.getOperationFactory();
    }

    @Override
    public long getOperationTimeout() {
        return underlying.getOperationTimeout();
    }

    @Override
    public boolean isDaemon() {
        return underlying.isDaemon();
    }

    @Override
    public boolean useNagleAlgorithm() {
        return underlying.useNagleAlgorithm();
    }

    @Override
    public Collection<ConnectionObserver> getInitialObservers() {
        return underlying.getInitialObservers();
    }

    @Override
    public FailureMode getFailureMode() {
        return underlying.getFailureMode();
    }

    @Override
    public Transcoder<Object> getDefaultTranscoder() {
        return underlying.getDefaultTranscoder();
    }

    @Override
    public boolean shouldOptimize() {
        return underlying.shouldOptimize();
    }

    @Override
    public int getReadBufSize() {
        return underlying.getReadBufSize();
    }

    @Override
    public HashAlgorithm getHashAlg() {
        return underlying.getHashAlg();
    }

    @Override
    public long getMaxReconnectDelay() {
        return underlying.getMaxReconnectDelay();
    }

    @Override
    public AuthDescriptor getAuthDescriptor() {
        return underlying.getAuthDescriptor();
    }

    @Override
    public int getTimeoutExceptionThreshold() {
        return underlying.getTimeoutExceptionThreshold();
    }

    @Override
    public long getAuthWaitTime() {
        return underlying.getAuthWaitTime();
    }
}
