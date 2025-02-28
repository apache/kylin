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
package org.apache.kylin.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.service.TableService;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

@MetadataInfo
class DeleteDuringQueryTest {
    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());

    @Test
    void testModelSize() {
        List<NDataModel> models = new ArrayList<>();
        long res = ReflectionTestUtils.invokeMethod(tableService, "getStorageSize", "default", models, null);
        Assert.assertEquals(0L, res);
        NDataModel model = new NDataModel();
        model.setUuid("testUUid");
        models.add(model);
        long res2 = ReflectionTestUtils.invokeMethod(tableService, "getStorageSize", "default", models, null);
        Assert.assertEquals(0L, res2);
    }


    @Test
    void tesTableColumnType() {
        List<NDataModel> models = new ArrayList<>();
        TableDesc table = new TableDesc();
        Pair<Set<String>, Set<String>> res = ReflectionTestUtils.invokeMethod(tableService, "getTableColumnType", "default", table, models);
        Assert.assertEquals(Boolean.TRUE, res.getFirst().size()==0);
        NDataModel model = new NDataModel();
        model.setUuid("testUUid");
        models.add(model);
        Pair<Set<String>, Set<String>> res2 = ReflectionTestUtils.invokeMethod(tableService, "getTableColumnType", "default", table, models);
        Assert.assertEquals(Boolean.TRUE, res2.getFirst().size()==0);
    }
}
