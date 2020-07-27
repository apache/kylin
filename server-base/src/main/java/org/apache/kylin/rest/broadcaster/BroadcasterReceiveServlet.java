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

package org.apache.kylin.rest.broadcaster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.util.EncryptUtil;

/**
 */
public class BroadcasterReceiveServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Pattern PATTERN = Pattern.compile("/(.+)/(.+)/(.+)");
    private static final Pattern PATTERN2 = Pattern.compile("/(.+)/(.+)");
    private final BroadcasterHandler handler;

    public BroadcasterReceiveServlet(BroadcasterHandler handler) {
        this.handler = handler;
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        handle(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        handle(req, resp);
    }

    private void handle(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        final String startString = "/kylin/api/cache";
        final String requestURI = req.getRequestURI();
        final String substring = requestURI.substring(requestURI.indexOf(startString) + startString.length());
        final Matcher matcher = PATTERN.matcher(substring);
        final Matcher matcher2 = PATTERN2.matcher(substring);

        if (matcher.matches()) {
            String type = matcher.group(1);
            String cacheKey = matcher.group(2);
            String event = matcher.group(3);
            if (handler != null) {
                handler.handle(type, cacheKey, event);
            }
            resp.getWriter().write("Encrypted(type:" + EncryptUtil.encrypt(type) + " name:" + EncryptUtil.encrypt(cacheKey)
                    + " event:" + EncryptUtil.encrypt(event) + ")");
        } else if (matcher2.matches()) {
            String type = matcher2.group(1);
            String event = matcher2.group(2);
            BufferedReader br = new BufferedReader(new InputStreamReader(req.getInputStream(), "utf-8"));
            String cacheKey = br.readLine();
            br.close();
            if (handler != null) {
                handler.handle(type, cacheKey, event);
            }
            resp.getWriter().write("Encrypted(type:" + EncryptUtil.encrypt(type) + " name:" + EncryptUtil.encrypt(cacheKey)
                    + " event:" + EncryptUtil.encrypt(event) + ")");
        } else {
            resp.getWriter().write("not valid uri");
        }
        resp.getWriter().close();
    }

    public interface BroadcasterHandler {

        void handle(String type, String name, String event);
    }
}
