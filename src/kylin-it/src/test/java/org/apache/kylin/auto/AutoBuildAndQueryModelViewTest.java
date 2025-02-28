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

package org.apache.kylin.auto;

import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.query.util.ConvertToComputedColumn;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class AutoBuildAndQueryModelViewTest extends SuggestTestBase {

    @Test
    public void testReplaceCc() {
        String sqlPattern = "select seller_id, ITEM_COUNT * PRICE from {view}";
        String expectedSql = "select seller_id, {cc1} from {view}";

        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select sum(ITEM_COUNT * PRICE) from test_kylin_fact" }, true);

        NDataModel targetModel = context.getModelContexts().get(0).getTargetModel();
        String targetModelAlias = targetModel.getAlias();
        String sql = sqlPattern.replace("{view}", "newten." + targetModelAlias);
        String transformed = new ConvertToComputedColumn().transform(sql, getProject(), "default");

        expectedSql = expectedSql.replace("{view}", "newten." + targetModelAlias);
        int idx = 1;
        for (ComputedColumnDesc cc : targetModel.getComputedColumnDescs()) {
            expectedSql = expectedSql.replace("{cc" + idx + "}",
                    StringHelper.doubleQuote(targetModelAlias) + "." + StringHelper.doubleQuote(cc.getColumnName()));
            idx++;
        }
        Assert.assertEquals(expectedSql, transformed);
    }
}
