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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StateSwitchTest extends NLocalFileMetadataTestCase {

    private KylinConfig kylinConfig;

    @Before
    public void setup() {
        createTestMetadata();
        kylinConfig = getTestConfig();
        kylinConfig.setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testQueryShareStateManager() {
        QueryShareStateManager manager;
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)) {
            kylinConfig.setProperty("kylin.query.share-state-switch-implement", "jdbc");
            manager = QueryShareStateManager.getInstance();

            String stateName = "QueryLimit";
            String stateVal = "false";
            String stateValFromDatabase = manager.getState(stateName);
            Assert.assertEquals(stateVal, stateValFromDatabase);

            String updateStateVal = "true";
            List<String> instanceList = Collections.singletonList(AddressUtil.concatInstanceName());
            manager.setState(instanceList, stateName, updateStateVal);
            stateValFromDatabase = manager.getState(stateName);
            Assert.assertEquals(updateStateVal, stateValFromDatabase);
        }
    }

    @Test
    public void testMetadataStateSwitchFunction() {
        MetadataStateSwitch metadataStateSwitch = new MetadataStateSwitch();

        String instanceName = "127.0.0.1:8080";
        String stateName = "QueryLimit";
        String stateVal = "false";

        // if there is no data in metadata
        metadataStateSwitch.put(instanceName, stateName, stateVal);
        String stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(null, stateValFromDatabase);

        Map<String, String> initStateMap = new HashMap<>();
        initStateMap.put(stateName, stateVal);

        // if no data existed when init
        metadataStateSwitch.init(instanceName, initStateMap);
        stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(stateVal, stateValFromDatabase);

        // if data existed when init
        metadataStateSwitch.init(instanceName, initStateMap);
        stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(stateVal, stateValFromDatabase);

        String updateStateVal = "true";
        metadataStateSwitch.put(instanceName, stateName, updateStateVal);
        stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(updateStateVal, stateValFromDatabase);

        // test the 'else' branch in init method
        metadataStateSwitch.init(instanceName, initStateMap);
        stateValFromDatabase = metadataStateSwitch.get(instanceName, stateName);
        Assert.assertEquals(stateVal, stateValFromDatabase);
    }

    @Test
    public void testMapStringConvert() {
        Map<String, String> testMap = null;
        String convertedStr;
        convertedStr = MetadataStateSwitch.convertMapToString(testMap);
        Assert.assertEquals("", convertedStr);
        testMap = new HashMap<>();
        convertedStr = MetadataStateSwitch.convertMapToString(testMap);
        Assert.assertEquals("", convertedStr);

        String testStr = null;
        Map<String, String> convertedMap;
        convertedMap = MetadataStateSwitch.convertStringToMap(testStr);
        Assert.assertEquals(0, convertedMap.size());
        testStr = "";
        convertedMap = MetadataStateSwitch.convertStringToMap(testStr);
        Assert.assertEquals(0, convertedMap.size());

        testMap.clear();
        testMap.put("QueryLimit", "true");
        convertedStr = MetadataStateSwitch.convertMapToString(testMap);
        convertedStr = convertedStr.replace("\u0000", "");
        convertedMap = MetadataStateSwitch.convertStringToMap(convertedStr);
        Assert.assertEquals(1, convertedMap.size());
        Assert.assertEquals("true", convertedMap.get("QueryLimit"));

        testMap.clear();
        testMap.put("QueryLimit", "false");
        convertedStr = MetadataStateSwitch.convertMapToString(testMap);
        convertedStr = convertedStr.replace("\u0000", "");
        convertedMap = MetadataStateSwitch.convertStringToMap(convertedStr);
        Assert.assertEquals(1, convertedMap.size());
        Assert.assertEquals("false", convertedMap.get("QueryLimit"));
    }

}
