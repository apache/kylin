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

package org.apache.kylin.stream.coordinator;

import org.apache.kylin.stream.core.source.StreamingTableSourceInfo;

public class StreamingCubeInfo {
    private String cubeName;
    private StreamingTableSourceInfo streamingTableSourceInfo;
    private int numOfConsumerTasks;

    public StreamingCubeInfo(String cubeName, StreamingTableSourceInfo streamingTableSourceInfo, int numOfConsumerTasks) {
        this.cubeName = cubeName;
        this.streamingTableSourceInfo = streamingTableSourceInfo;
        this.numOfConsumerTasks = numOfConsumerTasks;
    }

    public String getCubeName() {
        return cubeName;
    }

    public StreamingTableSourceInfo getStreamingTableSourceInfo() {
        return streamingTableSourceInfo;
    }

    public int getNumOfConsumerTasks() {
        return numOfConsumerTasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamingCubeInfo that = (StreamingCubeInfo) o;

        if (numOfConsumerTasks != that.numOfConsumerTasks) return false;
        if (cubeName != null ? !cubeName.equals(that.cubeName) : that.cubeName != null) return false;
        return streamingTableSourceInfo != null ? streamingTableSourceInfo.equals(that.streamingTableSourceInfo) : that.streamingTableSourceInfo == null;

    }

    @Override
    public int hashCode() {
        int result = cubeName != null ? cubeName.hashCode() : 0;
        result = 31 * result + (streamingTableSourceInfo != null ? streamingTableSourceInfo.hashCode() : 0);
        result = 31 * result + numOfConsumerTasks;
        return result;
    }
}
