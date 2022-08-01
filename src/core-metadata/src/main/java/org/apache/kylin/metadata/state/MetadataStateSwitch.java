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

package org.apache.kylin.metadata.state;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.state.IStateSwitch;

public class MetadataStateSwitch implements IStateSwitch {
    private JdbcShareStateStore jdbcResourceStateStore;

    public MetadataStateSwitch() {
        jdbcResourceStateStore = JdbcShareStateStore.getInstance();
    }

    @Override
    public void init(String instanceName, Map<String, String> initStateMap) {
        String serializeToString = convertMapToString(initStateMap);
        ShareStateInfo shareStateInfo = jdbcResourceStateStore.selectShareStateByInstanceName(instanceName);
        if(shareStateInfo == null) {
            jdbcResourceStateStore.insert(instanceName, serializeToString);
        } else {
            if(!serializeToString.equals(shareStateInfo.getShareState())) {
                Map<String, String> curStateMap = convertStringToMap(shareStateInfo.getShareState());
                initStateMap.forEach(curStateMap::put);
                jdbcResourceStateStore.update(instanceName, convertMapToString(curStateMap));
            }
        }
    }

    @Override
    public void put(String instanceName, String stateName, String stateValue) {
        ShareStateInfo shareStateInfo = jdbcResourceStateStore.selectShareStateByInstanceName(instanceName);
        if(shareStateInfo != null) {
            Map<String, String> stateMap = convertStringToMap(shareStateInfo.getShareState());
            stateMap.put(stateName, stateValue);
            String serializeToString = convertMapToString(stateMap);
            jdbcResourceStateStore.update(instanceName, serializeToString);
        }
    }

    @Override
    public String get(String instanceName, String stateName) {
        ShareStateInfo shareStateInfo = jdbcResourceStateStore.selectShareStateByInstanceName(instanceName);
        String stateValue = null;
        if(shareStateInfo != null) {
            Map<String, String> stateMap = convertStringToMap(shareStateInfo.getShareState());
            stateValue = stateMap.get(stateName);
        }
        return stateValue;
    }

    public static String convertMapToString(Map<String, String> map) {
        if(map == null || map.isEmpty()) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        map.forEach((key, val) -> stringBuilder.append(key).append("-").append(val).append(";"));
        // if PostgreSQl is employed for metadata, to avoid error "invalid byte sequence for encoding "UTF8": 0x00"
        return stringBuilder.toString().replace("\u0000", "");
    }

    public static Map<String, String> convertStringToMap(String string) {
        Map<String, String> result = new HashMap<>();
        if(string == null || string.isEmpty()) {
            return result;
        }
        String[] nameAndValueArray = string.split(";");
        for (String nameAndValue : nameAndValueArray) {
            String[] pair = nameAndValue.split("-");
            result.put(pair[0].trim(), pair[1].trim());
        }
        return result;
    }
}
