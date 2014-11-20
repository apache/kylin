/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.rest;

import java.io.File;

import org.apache.catalina.Context;
import org.apache.catalina.core.AprLifecycleListener;
import org.apache.catalina.core.StandardServer;
import org.apache.catalina.deploy.ErrorPage;
import org.apache.catalina.startup.Tomcat;

import com.kylinolap.rest.util.ClasspathUtil;

public class DebugTomcat {

    public static void main(String[] args) throws Exception {
        if (args.length >= 1) {
            System.setProperty("kylin.metadata.url", args[0]);
        }
        int port = 7070;
        if (args.length >= 2) {
            port = Integer.parseInt(args[1]);
        }

        ClasspathUtil.addClasspath(new File("../examples/test_case_data").getAbsolutePath());
        ClasspathUtil.addClasspath(new File("../examples/test_case_data/hadoop-site").getAbsolutePath());
        String webBase = new File("../webapp/app").getAbsolutePath();
        String apiBase = new File("src/main/webapp").getAbsolutePath();

        Tomcat tomcat = new Tomcat();
        tomcat.setPort(port);
        tomcat.setBaseDir(".");

        // Add AprLifecycleListener
        StandardServer server = (StandardServer) tomcat.getServer();
        AprLifecycleListener listener = new AprLifecycleListener();
        server.addLifecycleListener(listener);

        tomcat.addWebapp("/kylin", apiBase);
        Context webContext = tomcat.addWebapp("/", webBase);
        ErrorPage notFound = new ErrorPage();
        notFound.setErrorCode(404);
        notFound.setLocation("/index.html");
        webContext.addErrorPage(notFound);
        webContext.addWelcomeFile("index.html");

        // tomcat start
        tomcat.start();
        tomcat.getServer().await();
    }

}
