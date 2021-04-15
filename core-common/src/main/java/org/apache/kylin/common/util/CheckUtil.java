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
package org.apache.kylin.common.util;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckUtil {
    public static final Logger logger = LoggerFactory.getLogger(CheckUtil.class);
    private static final Random rand = new Random();

    private CheckUtil(){
        throw new IllegalStateException("Class CheckUtil is an utility class !");
    }

    public static boolean checkCondition(boolean condition, String message, Object... args) {
        if (condition) {
            return true;
        } else {
            logger.debug(message, args);
            return false;
        }
    }

    public static int randomAvailablePort(int minPort, int maxPort) {
        for (int i = 0; i < 100; i++) {
            int p = minPort + rand.nextInt(maxPort - minPort);
            if (checkPortAvailable(p))
                return p;
        }
        throw new IllegalArgumentException("Failed to get random available port between [" + minPort + "," + maxPort + ")");
    }

    /**
     * Checks to see if a specific port is available.
     *
     * @param port the port to check for availability
     */
    public static boolean checkPortAvailable(int port) {

        try(ServerSocket ss = new ServerSocket(port);
            DatagramSocket ds = new DatagramSocket(port);
            ) {
            ss.setReuseAddress(true);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            logger.error("Exception in checking port, should be ignored.");
        }

        return false;
    }

    public static boolean equals(String s1, String s2) {
        if (s1 != null && s2 != null) {
            return s1.trim().equalsIgnoreCase(s2.trim());
        }
        return s1 == null && s2 == null;
    }

    public static <T> boolean equals(T o1, T o2) {
        if (o1 != null && o2 != null) {
            return o1.equals(o2);
        }
        return o1 == null && o2 == null;
    }
}
