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

package org.apache.kylin.rest.controller;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author shaoshi
 */
//TODO: break this into unit tests
@Ignore("ServiceTestBase has been changed to local meta tests, thus not suitable for hive related tests")
public class ITTableControllerTest extends ServiceTestBase {

    private TableController tableController;
    private CubeDescController cubeDescController;

    @Autowired
    CubeService cubeService;

    @Before
    public void setup() throws Exception {
        super.setup();

        tableController = new TableController();
        tableController.setCubeService(cubeService);
    }

    @Test
    public void testBasics() throws IOException {
        List<TableDesc> tables = tableController.getHiveTables(true, "default");

        Assert.assertTrue(tables != null && tables.size() > 0);

        TableDesc factTable = null;
        for (TableDesc t : tables) {
            if (t.getName().equalsIgnoreCase("test_kylin_fact")) {
                factTable = t;
                break;
            }
        }
        Assert.assertNotNull(factTable);

        Map<String, String[]> loadResult = tableController.loadHiveTable("test_kylin_fact,TEST_CATEGORY_GROUPINGS", "default");
        Assert.assertNotNull(loadResult);

        Assert.assertTrue(loadResult.get("result.loaded").length ==2);
    }
}
