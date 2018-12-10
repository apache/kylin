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

package org.apache.kylin.stream.coordinator.client;

import java.net.InetAddress;
import java.net.NetworkInterface;

import org.apache.kylin.stream.coordinator.Coordinator;
import org.apache.kylin.stream.coordinator.StreamMetadataStore;
import org.apache.kylin.stream.core.model.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorClientFactory.class);

    public static CoordinatorClient createCoordinatorClient(StreamMetadataStore streamMetadataStore) {
        if (isCoordinatorCoLocate(streamMetadataStore)) {
            return Coordinator.getInstance();
        } else {
            return new HttpCoordinatorClient(streamMetadataStore);
        }
    }

    private static boolean isCoordinatorCoLocate(StreamMetadataStore streamMetadataStore) {
        try {
            Node coordinatorNode = streamMetadataStore.getCoordinatorNode();
            if (coordinatorNode == null) {
                logger.warn("no coordinator node registered");
                return true;
            }
            InetAddress inetAddress = InetAddress.getByName(coordinatorNode.getHost());
            return NetworkInterface.getByInetAddress(inetAddress) != null;
        } catch (Exception e) {
            logger.error("error when ");
        }
        return true;
    }
}
