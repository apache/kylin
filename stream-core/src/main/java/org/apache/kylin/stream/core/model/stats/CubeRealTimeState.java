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

package org.apache.kylin.stream.core.model.stats;

import java.util.Map;

import org.apache.kylin.stream.core.model.Node;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class CubeRealTimeState {
    @JsonProperty("cube_name")
    private String cubeName;

    @JsonProperty("receiver_cube_real_time_states")
    private Map<Integer, Map<Node, ReceiverCubeRealTimeState>> receiverCubeStateMap = Maps.newHashMap();

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public Map<Integer, Map<Node, ReceiverCubeRealTimeState>> getReceiverCubeStateMap() {
        return receiverCubeStateMap;
    }

    public void setReceiverCubeStateMap(Map<Integer, Map<Node, ReceiverCubeRealTimeState>> receiverCubeStateMap) {
        this.receiverCubeStateMap = receiverCubeStateMap;
    }
}
