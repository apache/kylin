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
package org.apache.kylin.tool.util;

import org.apache.kylin.common.util.Unsafe;

public class ScreenPrintUtil {
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RESET = "\u001B[0m";

    private ScreenPrintUtil() {

    }

    public static void println(String msg) {
        System.out.println(msg);
    }

    private static void printlnColorMsg(String type, String msg) {
        System.out.println(type + msg + ANSI_RESET);
    }

    public static void printlnGreen(String msg) {
        printlnColorMsg(ANSI_GREEN, msg);
    }

    public static void printlnRed(String msg) {
        printlnColorMsg(ANSI_RED, msg);
    }

    public static void printlnYellow(String msg) {
        printlnColorMsg(ANSI_YELLOW, msg);
    }

    public static void printlnBlue(String msg) {
        printlnColorMsg(ANSI_BLUE, msg);
    }

    public static void systemExitWhenMainThread(int code) {
        if (isMainThread()) {
            Unsafe.systemExit(code);
        }
    }

    public static boolean isMainThread() {
        return "main".equalsIgnoreCase(Thread.currentThread().getName());
    }
}
