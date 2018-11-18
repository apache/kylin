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

package org.apache.kylin.storage.druid.http;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kylin.storage.druid.http.pool.ChannelResourceFactory;
import org.apache.kylin.storage.druid.http.pool.ResourcePool;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HttpClientFactory {

    public static HttpClient createClient(HttpClientConfig config) {
        try {
            final HashedWheelTimer timer = new HashedWheelTimer(
                    new ThreadFactoryBuilder().setDaemon(true)
                            .setNameFormat("HttpClient-Timer-%s")
                            .build(),
                    100,
                    TimeUnit.MILLISECONDS
            );
            timer.start();

            final NioClientBossPool bossPool = new NioClientBossPool(
                    Executors.newCachedThreadPool(
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("HttpClient-Netty-Boss-%s")
                                    .build()
                    ),
                    config.getBossPoolSize(),
                    timer,
                    ThreadNameDeterminer.CURRENT
            );

            final NioWorkerPool workerPool = new NioWorkerPool(
                    Executors.newCachedThreadPool(
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("HttpClient-Netty-Worker-%s")
                                    .build()
                    ),
                    config.getWorkerPoolSize(),
                    ThreadNameDeterminer.CURRENT
            );

            final ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(bossPool, workerPool));
            bootstrap.setOption("keepAlive", true);
            bootstrap.setPipelineFactory(new HttpClientPipelineFactory());

            InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());

            return new NettyHttpClient(
                    new ResourcePool<>(
                            new ChannelResourceFactory(bootstrap),
                            config.getNumConnections()
                    ),
                    config.getReadTimeout(),
                    config.getCompressionCodec(),
                    timer
            );

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
