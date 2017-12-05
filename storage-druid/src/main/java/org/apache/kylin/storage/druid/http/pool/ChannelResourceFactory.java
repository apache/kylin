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
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;

public class ChannelResourceFactory implements ResourceFactory<String, ChannelFuture> {
    private static final Logger log = LoggerFactory.getLogger(ChannelResourceFactory.class);

    private final ClientBootstrap bootstrap;

    public ChannelResourceFactory(ClientBootstrap bootstrap) {
        this.bootstrap = Preconditions.checkNotNull(bootstrap, "bootstrap");
    }

    @Override
    public ChannelFuture generate(final String hostname) {
        log.info("Generating: {}", hostname);
        URL url;
        try {
            url = new URL(hostname);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        final String host = url.getHost();
        final int port = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
        return bootstrap.connect(new InetSocketAddress(host, port));
    }

    @Override
    public boolean isGood(ChannelFuture resource) {
        Channel channel = resource.awaitUninterruptibly().getChannel();

        boolean isSuccess = resource.isSuccess();
        boolean isConnected = channel.isConnected();
        boolean isOpen = channel.isOpen();

        if (log.isTraceEnabled()) {
            log.trace("isGood = isSucess[{}] && isConnected[{}] && isOpen[{}]", isSuccess, isConnected, isOpen);
        }

        return isSuccess && isConnected && isOpen;
    }

    @Override
    public void close(ChannelFuture resource) {
        log.trace("Closing");
        resource.awaitUninterruptibly().getChannel().close();
    }
}
