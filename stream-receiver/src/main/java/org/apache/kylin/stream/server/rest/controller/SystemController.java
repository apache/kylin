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

package org.apache.kylin.stream.server.rest.controller;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.stream.server.StreamingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Handle system requests.
 */
@Controller
@RequestMapping(value = "/system")
public class SystemController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(SystemController.class);

    private StreamingServer streamingServer;

    public SystemController() {
        streamingServer = StreamingServer.getInstance();
    }

    @RequestMapping(value = "/logLevel/{loggerName}/{logLevel}", method = RequestMethod.PUT, produces = {
            "application/json" })
    @ResponseBody
    public void setLogLevel(@PathVariable(value = "loggerName") String loggerName,
            @PathVariable(value = "logLevel") String logLevel) {
        // we know it use log4j 
        org.apache.log4j.Logger logger = org.apache.log4j.LogManager.getLogger(loggerName);
        org.apache.log4j.Level level = org.apache.log4j.Level.toLevel(logLevel);
        logger.setLevel(level);
    }

    @RequestMapping(value = "/logLevel/{loggerName}", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public String getLogLevel(@PathVariable(value = "loggerName") String loggerName) {
        org.apache.log4j.Logger logger = org.apache.log4j.LogManager.getLogger(loggerName);
        org.apache.log4j.Level level = logger.getEffectiveLevel();
        if (level != null) {
            return level.toString();
        }
        return null;
    }

    @RequestMapping(value = "/threadDump", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public void threadDump(HttpServletResponse response) {
        response.setContentType("text/plain;charset=utf-8");
        try (OutputStream outputStream = response.getOutputStream()) {
            printThreadInfo(new PrintStream(outputStream, false, "UTF-8"), "Thread Dump");
        } catch (IOException e) {
            logger.error("exception when get stack trace", e);
        }
    }

    private static String getTaskName(long id, String name) {
        if (name == null) {
            return Long.toString(id);
        }
        return id + " (" + name + ")";
    }

    private static ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    /**
     * Print all of the thread's information and stack traces.
     *
     * @param stream the stream to
     * @param title a string title for the stack trace
     */
    public synchronized static void printThreadInfo(PrintStream stream, String title) {
        final int stackDepth = 20;
        boolean contention = threadBean.isThreadContentionMonitoringEnabled();
        long[] threadIds = threadBean.getAllThreadIds();
        stream.println("Process Thread Dump: " + title);
        stream.println(threadIds.length + " active threads");
        for (long tid : threadIds) {
            ThreadInfo info = threadBean.getThreadInfo(tid, stackDepth);
            if (info == null) {
                stream.println("  Inactive");
                continue;
            }
            stream.println("Thread " + getTaskName(info.getThreadId(), info.getThreadName()) + ":");
            Thread.State state = info.getThreadState();
            stream.println("  State: " + state);
            stream.println("  Blocked count: " + info.getBlockedCount());
            stream.println("  Waited count: " + info.getWaitedCount());
            if (contention) {
                stream.println("  Blocked time: " + info.getBlockedTime());
                stream.println("  Waited time: " + info.getWaitedTime());
            }
            if (state == Thread.State.WAITING) {
                stream.println("  Waiting on " + info.getLockName());
            } else if (state == Thread.State.BLOCKED) {
                stream.println("  Blocked on " + info.getLockName());
                stream.println("  Blocked by " + getTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
            }
            stream.println("  Stack:");
            for (StackTraceElement frame : info.getStackTrace()) {
                stream.println("    " + frame.toString());
            }
        }
        stream.flush();
    }
}
