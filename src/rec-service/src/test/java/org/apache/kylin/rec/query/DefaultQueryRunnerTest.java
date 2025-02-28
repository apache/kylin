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

package org.apache.kylin.rec.query;

import java.util.Collection;
import java.util.Map;

import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.query.advisor.SQLAdvice;
import org.apache.kylin.rec.query.advisor.SqlSyntaxAdvisor;
import org.junit.Assert;
import org.junit.Test;

public class DefaultQueryRunnerTest extends AutoTestOnLearnKylinData {

    @Test
    public void testExecute() throws Exception {
        String[] sqls = new String[] { "select sum(price * item_count), part_dt from kylin_sales group by part_dt",
                "select price, item_count, part_dt from kylin_sales" };
        AbstractQueryRunner queryRunner1 = new QueryRunnerBuilder(proj, getTestConfig(), sqls).build();
        queryRunner1.execute();
        Map<String, Collection<OlapContext>> olapContexts = queryRunner1.getOlapContexts();
        Assert.assertEquals(2, olapContexts.size());

        Assert.assertEquals(1, olapContexts.get(sqls[0]).size());
        OlapContext olapContext1 = olapContexts.get(sqls[0]).iterator().next();
        Assert.assertNotNull(olapContext1.getTopNode());
        Assert.assertNull(olapContext1.getParentOfTopNode());
        Assert.assertEquals(0, olapContext1.getAllOlapJoins().size());

        Assert.assertEquals(1, olapContexts.get(sqls[1]).size());
        OlapContext olapContext2 = olapContexts.get(sqls[1]).iterator().next();
        Assert.assertNotNull(olapContext2.getTopNode());
        Assert.assertNotNull(olapContext2.getParentOfTopNode());
        Assert.assertEquals(0, olapContext2.getAllOlapJoins().size());
    }

    @Test
    public void testProposeWithMessage() {
        SqlSyntaxAdvisor sqlSyntaxAdvisor = new SqlSyntaxAdvisor();

        SQLResult sqlResult = new SQLResult();
        sqlResult.setMessage("Non-query expression encountered in illegal context");
        SQLAdvice advice = sqlSyntaxAdvisor.proposeWithMessage(sqlResult);
        Assert.assertEquals("Please modify it.", advice.getSuggestion());
        Assert.assertEquals("Not Supported SQL.", advice.getIncapableReason());

        sqlResult.setMessage("Encountered \"()\" at line 12, column 234. Was expecting one of: ");
        advice = sqlSyntaxAdvisor.proposeWithMessage(sqlResult);
        Assert.assertEquals("Please modify it.", advice.getSuggestion());
        Assert.assertEquals("Syntax error occurred at line (), column 12: \"234\". Please modify it.",
                advice.getIncapableReason());

        sqlResult.setMessage("default message");
        advice = sqlSyntaxAdvisor.proposeWithMessage(sqlResult);
        Assert.assertEquals("Please contact Community support for more details.", advice.getSuggestion());
        Assert.assertEquals("Something went wrong. default message", advice.getIncapableReason());

        sqlResult.setMessage(
                "From line 234, column 234 to line 23, column 234:  \"Object '234' not found( within '324')\nwhile executing SQL: \"234\"");
        advice = sqlSyntaxAdvisor.proposeWithMessage(sqlResult);
        Assert.assertEquals("Please modify it.", advice.getSuggestion());
        Assert.assertEquals("The SQL has syntax error:  \"Object '234' not found( within '324') ",
                advice.getIncapableReason());
    }
}
