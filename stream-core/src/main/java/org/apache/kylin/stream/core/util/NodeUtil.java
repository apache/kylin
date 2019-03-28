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

package org.apache.kylin.stream.core.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.stream.core.model.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class NodeUtil {
    private static final Logger logger = LoggerFactory.getLogger(NodeUtil.class);

    public static Node getCurrentNode(int defaultPort) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String configNodeStr = kylinConfig.getStreamingNode();
        Node result;
        if (configNodeStr != null) {
            result = Node.from(configNodeStr);
        } else {
            result = new Node(getLocalhostName(), defaultPort);
        }
        Map<String, String> nodeProperties =  kylinConfig.getStreamingNodeProperties();
        result.setProperties(nodeProperties);
        return result;
    }

    private static String getLocalhostName() {
        String host;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            host = addr.getCanonicalHostName();
        } catch (UnknownHostException e) {
            logger.error("Fail to get local ip address", e);
            host = "UNKNOWN";
        }
        return host;
    }
}
