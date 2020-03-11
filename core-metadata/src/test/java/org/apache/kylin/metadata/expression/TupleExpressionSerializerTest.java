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

package org.apache.kylin.metadata.expression;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class TupleExpressionSerializerTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
    }

    private TableDesc t = TableDesc.mockup("T");

    @Test
    public void testSerialization() {
        TblColRef colD = TblColRef.mockup(t, 1, "C1", "decimal");
        TblColRef colM = TblColRef.mockup(t, 2, "C2", "string");
        BigDecimal value = BigDecimal.valueOf(10L);

        ColumnTupleFilter colFilter = new ColumnTupleFilter(colD);
        ConstantTupleFilter constFilter = new ConstantTupleFilter("col");
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        compareFilter.addChild(colFilter);
        compareFilter.addChild(constFilter);

        ColumnTupleExpression colTuple = new ColumnTupleExpression(colM);
        NumberTupleExpression constTuple = new NumberTupleExpression(value);

        Pair<TupleFilter, TupleExpression> whenEntry = new Pair<TupleFilter, TupleExpression>(compareFilter, colTuple);
        CaseTupleExpression caseTuple = new CaseTupleExpression(Lists.newArrayList(whenEntry), constTuple);

        byte[] result = TupleExpressionSerializer.serialize(caseTuple, StringCodeSystem.INSTANCE);

        TupleExpression desTuple = TupleExpressionSerializer.deserialize(result, StringCodeSystem.INSTANCE);
        assertEquals(caseTuple, desTuple);
    }
}
