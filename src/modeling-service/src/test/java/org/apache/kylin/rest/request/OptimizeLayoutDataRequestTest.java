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
package org.apache.kylin.rest.request;

import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class OptimizeLayoutDataRequestTest {

    @Test
    public void testBasic() {
        val template = OptimizeLayoutDataRequest.template;
        val modelSetting = template.getModelOptimizationSetting();
        val layoutSettings = template.getLayoutDataOptimizationSettingList().get(0).getSetting();

        Assert.assertEquals("", template.getProject());

        Assert.assertEquals(0, modelSetting.getMaxCompactionFileSize());
        Assert.assertEquals(0, modelSetting.getMinCompactionFileSize());
        Assert.assertTrue(modelSetting.getZorderByColumns().contains("TABLE_NAME.COLUMN_NAME"));
        Assert.assertTrue(modelSetting.getRepartitionByColumns().contains("TABLE_NAME.COLUMN_NAME"));

        Assert.assertEquals(0, layoutSettings.getMaxCompactionFileSize());
        Assert.assertEquals(0, layoutSettings.getMinCompactionFileSize());
        Assert.assertTrue(layoutSettings.getZorderByColumns().contains("TABLE_NAME.COLUMN_NAME"));
        Assert.assertTrue(layoutSettings.getRepartitionByColumns().contains("TABLE_NAME.COLUMN_NAME"));
    }
}
