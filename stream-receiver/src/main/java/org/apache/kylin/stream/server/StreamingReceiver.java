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

package org.apache.kylin.stream.server;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.util.NodeUtil;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

public class StreamingReceiver {
    private static final Logger logger = LoggerFactory.getLogger(StreamingReceiver.class);
    private Server httpServer;

    public static void main(String[] args) {
        try {
            StreamingReceiver receiver = new StreamingReceiver();
            receiver.start();
        } catch (Exception e) {
            logger.error("streaming receiver start fail", e);
        }
    }

    private void start() throws Exception {
        if (System.getProperty("debug") != null) {
            setupDebugEnv();
        }
        startStreamingServer();
        startRpcServer();
        startHttpServer();
    }

    private void startStreamingServer() throws Exception {
        StreamingServer.getInstance().start();
    }

    private void startHttpServer() throws Exception {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        createAndConfigHttpServer(kylinConfig);

        ContextHandlerCollection contexts = new ContextHandlerCollection();

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/kylin");

        XmlWebApplicationContext ctx = new XmlWebApplicationContext();
        ctx.setConfigLocation("classpath:applicationContext.xml");
        ctx.refresh();
        DispatcherServlet dispatcher = new DispatcherServlet(ctx);
        context.addServlet(new ServletHolder(dispatcher), "/api/*");

        ContextHandler logContext = new ContextHandler("/kylin/logs");
        String logDir = getLogDir(kylinConfig);
        ResourceHandler logHandler = new ResourceHandler();
        logHandler.setResourceBase(logDir);
        logHandler.setDirectoriesListed(true);
        logContext.setHandler(logHandler);

        contexts.setHandlers(new Handler[] { context, logContext });
        httpServer.setHandler(contexts);
        httpServer.start();
        httpServer.join();
    }

    private void startRpcServer() throws Exception {
        // currently use http server as rpc server
    }

    private String getLogDir(KylinConfig kylinConfig) {
        String kylinHome = kylinConfig.getKylinHome();
        if (kylinHome == null) {
            kylinHome = System.getProperty("KYLIN_HOME");
        }
        return kylinHome + File.separator + "logs";
    }

    private void createAndConfigHttpServer(KylinConfig kylinConfig) {
        httpServer = new Server(createThreadPool(kylinConfig));
        ServerConnector httpConnector = getHttpConnector();
        httpConnector.setPort(getHttpPort());
        httpConnector.setIdleTimeout(30000);
        httpServer.addConnector(httpConnector);
    }

    private ServerConnector getHttpConnector() {
        return new ServerConnector(httpServer);
    }

    private ThreadPool createThreadPool(KylinConfig kylinConfig) {
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMinThreads(kylinConfig.getStreamingReceiverHttpMinThreads());
        threadPool.setMaxThreads(kylinConfig.getStreamingReceiverHttpMaxThreads());
        return threadPool;
    }

    private void setupDebugEnv() {
        try {
            Properties props = new Properties();
            props.load(new FileInputStream("../build/conf/kylin-tools-log4j.properties"));
            PropertyConfigurator.configure(props);
            KylinConfig.setSandboxEnvIfPossible();
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setProperty("kylin.stream.settled.storage",
                    "org.apache.kylin.stream.server.storage.LocalStreamStorage");
        } catch (Exception e) {
            logger.error("debug env setup fail", e);
        }
    }

    private int getHttpPort() {
        Node currentNode = NodeUtil.getCurrentNode(StreamingServer.DEFAULT_PORT);
        return currentNode.getPort();
    }
}
