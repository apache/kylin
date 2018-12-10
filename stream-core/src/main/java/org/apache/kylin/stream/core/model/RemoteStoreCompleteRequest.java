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

package org.apache.kylin.stream.core.model;

public class RemoteStoreCompleteRequest {
    private Node receiverNode;
    private String cubeName;
    private long segmentStart;
    private long segmentEnd;

    public Node getReceiverNode() {
        return receiverNode;
    }

    public void setReceiverNode(Node receiverNode) {
        this.receiverNode = receiverNode;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public long getSegmentStart() {
        return segmentStart;
    }

    public void setSegmentStart(long segmentStart) {
        this.segmentStart = segmentStart;
    }

    public long getSegmentEnd() {
        return segmentEnd;
    }

    public void setSegmentEnd(long segmentEnd) {
        this.segmentEnd = segmentEnd;
    }
}
