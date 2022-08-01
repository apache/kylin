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
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.state.IStateSwitch;
import org.apache.kylin.common.state.StateSwitchConstant;
import org.apache.kylin.common.util.AddressUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryShareStateManager {
    private static final Logger logger = LoggerFactory.getLogger(QueryShareStateManager.class);

    private static final String SHARE_STATE_CLOSE = "close";

    private static final String INSTANCE_NAME = AddressUtil.concatInstanceName();

    private IStateSwitch stateSwitch;

    public static QueryShareStateManager getInstance() {
        QueryShareStateManager instance = null;
        try {
            instance = Singletons.getInstance(QueryShareStateManager.class);
        } catch (RuntimeException e) {
            logger.error("QueryShareStateManager init failed: ", e);
        }
        return instance;
    }

    private QueryShareStateManager() {
        if (!isShareStateSwitchEnabled()) {
            return;
        }
        stateSwitch = new MetadataStateSwitch();

        Map<String, String> initStateMap = new HashMap<>();
        initStateMap.put(StateSwitchConstant.QUERY_LIMIT_STATE, "false");
        stateSwitch.init(INSTANCE_NAME, initStateMap);
    }

    public static boolean isShareStateSwitchEnabled() {
        return !SHARE_STATE_CLOSE.equals(KapConfig.getInstanceFromEnv().getShareStateSwitchImplement());
    }

    public void setState(List<String> instanceNameList, String stateName, String stateValue) {
        if (!isShareStateSwitchEnabled()) {
            return;
        }
        logger.info("Receive state set signal, instance:{}, stateName:{}, stateValue:{}", instanceNameList,
                stateName, stateValue);
        for (String instance : instanceNameList) {
            stateSwitch.put(instance, stateName, stateValue);
        }
    }

    public String getState(String stateName) {
        String stateValue = null;
        if (isShareStateSwitchEnabled()) {
            stateValue = stateSwitch.get(INSTANCE_NAME, stateName);
            logger.info("Get state value, instance:{}, stateName:{}, stateValue:{}", INSTANCE_NAME, stateName,
                    stateValue);
        }
        return stateValue;
    }

}
