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
package io.kyligence.kap.newten.clickhouse;

import java.net.URI;
import java.util.Locale;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.testcontainers.Testcontainers;

import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmbeddedHttpServer {
    private final Server server;
    public final URI serverUri;
    public final URI uriAccessedByDocker;

    public EmbeddedHttpServer(Server server, URI serverUri, URI uriAccessedByDocker) {
        this.server = server;
        this.serverUri = serverUri;
        this.uriAccessedByDocker = uriAccessedByDocker;
    }

    public static EmbeddedHttpServer startServer(String workingDir) throws Exception {
        int port = ClickHouseClassRule.getAvailablePort();
        log.debug("start http server on port: {}", port);
        Server server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        server.addConnector(connector);

        ContextHandler contextHandler = new ContextHandler();
        ResourceHandler contentResourceHandler = new ResourceHandler();
        contentResourceHandler.setDirectoriesListed(true);
        contentResourceHandler.setResourceBase(workingDir);
        contextHandler.setContextPath("/");
        contextHandler.setHandler(contentResourceHandler);
        server.setHandler(contextHandler);
        server.start();
        int listenedPort = connector.getLocalPort();
        String host = connector.getHost();
        if (host == null) {
            host = "localhost";
        }
        URI serverUri = new URI(String.format(Locale.ROOT, "http://%s:%d", host, listenedPort));
        Testcontainers.exposeHostPorts(listenedPort);
        URI uriAccessedByDocker = new URI(
                String.format(Locale.ROOT, "http://host.testcontainers.internal:%d", listenedPort));
        return new EmbeddedHttpServer(server, serverUri, uriAccessedByDocker);
    }

    public void stopServer() throws Exception {
        if (!server.isStopped()) {
            server.stop();
            server.join();
        }
    }
}
