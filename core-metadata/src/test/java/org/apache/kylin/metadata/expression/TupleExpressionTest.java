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
import static org.junit.Assert.fail;

import java.math.BigDecimal;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.exception.QueryOnCubeException;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class TupleExpressionTest extends LocalFileMetadataTestCase {

    private TableDesc t = TableDesc.mockup("T");

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @Test
    public void testReferDataType() {
        assertEquals(TupleExpression.referDataType(null, DataType.getType("int")), DataType.getType("bigint"));
        assertEquals(TupleExpression.referDataType(DataType.getType("int"), null), DataType.getType("bigint"));
        assertEquals(TupleExpression.referDataType(DataType.getType("int"), DataType.getType("bigint")),
                DataType.getType("bigint"));
        assertEquals(TupleExpression.referDataType(DataType.getType("int"), DataType.getType("double")),
                DataType.getType("double"));
        assertEquals(TupleExpression.referDataType(DataType.getType("decimal"), DataType.getType("bigint")),
                DataType.getType("decimal"));
        assertEquals(TupleExpression.referDataType(DataType.getType("decimal"), DataType.getType("double")),
                DataType.getType("decimal"));
    }

    @Test
    public void testBinary() {
        BigDecimal value1 = BigDecimal.valueOf(10L);
        BigDecimal value2 = BigDecimal.valueOf(10L);
        TblColRef col1 = TblColRef.mockup(t, 1, "C1", "decimal");
        TblColRef col2 = TblColRef.mockup(t, 2, "C2", "decimal");

        ConstantTupleExpression constTuple1 = new ConstantTupleExpression(DataType.getType("decimal"), value1);
        ConstantTupleExpression constTuple2 = new ConstantTupleExpression(DataType.getType("decimal"), value2);
        ColumnTupleExpression colTuple1 = new ColumnTupleExpression(col1);
        ColumnTupleExpression colTuple2 = new ColumnTupleExpression(col2);

        BinaryTupleExpression biTuple1 = new BinaryTupleExpression(DataType.getType("decimal"),
                TupleExpression.ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(constTuple1, colTuple1));
        biTuple1.verify();

        BinaryTupleExpression biTuple2 = new BinaryTupleExpression(DataType.getType("decimal"),
                TupleExpression.ExpressionOperatorEnum.DIVIDE, Lists.newArrayList(constTuple2, colTuple2));
        try {
            biTuple2.verify();
            fail("QueryOnCubeException should be thrown，That the right side of the BinaryTupleExpression owns columns is not supported for /");
        } catch (QueryOnCubeException e) {
        }

        biTuple2 = new BinaryTupleExpression(DataType.getType("decimal"), TupleExpression.ExpressionOperatorEnum.DIVIDE,
                Lists.newArrayList(colTuple2, constTuple2));
        biTuple2.verify();

        BinaryTupleExpression biTuple = new BinaryTupleExpression(DataType.getType("decimal"),
                TupleExpression.ExpressionOperatorEnum.MULTIPLE,
                Lists.<TupleExpression> newArrayList(biTuple1, biTuple2));
        try {
            biTuple.verify();
            fail("QueryOnCubeException should be thrown，That both of the two sides of the BinaryTupleExpression own columns is not supported for *");
        } catch (QueryOnCubeException e) {
        }
    }
}