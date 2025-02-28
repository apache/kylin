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

package org.apache.kylin.rec.model;

import java.lang.reflect.Field;

import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class QueryScopeProposerTest extends NLocalWithSparkSessionTest {

    @Test
    public void testTransferToNamedColumn() throws Exception {
        final String sql = "select order_id from TEST_KYLIN_FACT";
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), new String[] { sql }, true);
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        QueryScopeProposer.ScopeBuilder scopeBuilder = new QueryScopeProposer.ScopeBuilder(
                context.getProposedModels().get(0), modelContext);

        TblColRef col1 = TblColRef.mockup(TableDesc.mockup("DEFAULT.A_B"), 1, "C", "double");
        Field f1 = col1.getClass().getDeclaredField("identity");
        Unsafe.changeAccessibleObject(f1, true);
        f1.set(col1, "A_B.C");
        TblColRef col2 = TblColRef.mockup(TableDesc.mockup("DEFAULT.A"), 2, "B_C", "double");
        Field f2 = col2.getClass().getDeclaredField("identity");
        Unsafe.changeAccessibleObject(f2, true);
        f2.set(col2, "A.B_C");

        NDataModel.NamedColumn namedColumn1 = scopeBuilder.transferToNamedColumn(col1, null);
        NDataModel.NamedColumn namedColumn2 = scopeBuilder.transferToNamedColumn(col2, null);
        Assert.assertNotEquals(namedColumn1, namedColumn2);
        Assert.assertEquals("C", namedColumn1.getName());
        Assert.assertEquals("B_C", namedColumn2.getName());
    }
}
