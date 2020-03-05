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
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.expression.TupleExpression.ExpressionOperatorEnum;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class ExpressionCountDistributorTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @Test
    public void testDataType() {
        TblColRef c = PowerMockito.mock(TblColRef.class);
        PowerMockito.when(c.getType()).thenReturn(DataType.getType("int"));
        IEvaluatableTuple evaluatableTuple = PowerMockito.mock(IEvaluatableTuple.class);
        IFilterCodeSystem filterCodeSystem = PowerMockito.mock(IFilterCodeSystem.class);
        TupleExpression t0 = new ColumnTupleExpression(c);
        t0 = PowerMockito.spy(t0);
        PowerMockito.when(t0.calculate(evaluatableTuple, filterCodeSystem)).thenReturn(1);

        TupleFilter f0 = PowerMockito.mock(CompareTupleFilter.class);

        List<Pair<TupleFilter, TupleExpression>> whenList = Lists.newArrayList();
        whenList.add(new Pair<>(f0, t0));

        { // bigint
            TupleExpression t1 = new ConstantTupleExpression(1);

            TupleExpression ret = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t0, t1);
            assertEquals((long) 2, ret.calculate(evaluatableTuple, filterCodeSystem));
            ret = new BinaryTupleExpression(ExpressionOperatorEnum.MINUS, t0, t1);
            assertEquals((long) 0, ret.calculate(evaluatableTuple, filterCodeSystem));
            ret = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, t0, t1);
            assertEquals((long) 1, ret.calculate(evaluatableTuple, filterCodeSystem));
            ret = new BinaryTupleExpression(ExpressionOperatorEnum.DIVIDE, t0, t1);
            assertEquals(1.0, ret.calculate(evaluatableTuple, filterCodeSystem));

            t1 = new ConstantTupleExpression(2);
            ret = new CaseTupleExpression(whenList, t1);

            PowerMockito.when(f0.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(true);
            assertEquals((long) 1, ret.calculate(evaluatableTuple, filterCodeSystem));
            PowerMockito.when(f0.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(false);
            assertEquals((long) 2, ret.calculate(evaluatableTuple, filterCodeSystem));
        }

        { // double
            TupleExpression t1 = new ConstantTupleExpression(1.0);

            TupleExpression ret = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t0, t1);
            assertEquals(2.0, ret.calculate(evaluatableTuple, filterCodeSystem));
            ret = new BinaryTupleExpression(ExpressionOperatorEnum.MINUS, t0, t1);
            assertEquals(0.0, ret.calculate(evaluatableTuple, filterCodeSystem));
            ret = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, t0, t1);
            assertEquals(1.0, ret.calculate(evaluatableTuple, filterCodeSystem));
            ret = new BinaryTupleExpression(ExpressionOperatorEnum.DIVIDE, t0, t1);
            assertEquals(1.0, ret.calculate(evaluatableTuple, filterCodeSystem));

            t1 = new ConstantTupleExpression(2.0);
            ret = new CaseTupleExpression(whenList, t1);

            PowerMockito.when(f0.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(true);
            assertEquals(1.0, ret.calculate(evaluatableTuple, filterCodeSystem));
            PowerMockito.when(f0.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(false);
            assertEquals(2.0, ret.calculate(evaluatableTuple, filterCodeSystem));
        }

        { // decimal
            TupleExpression t1 = new ConstantTupleExpression(new BigDecimal(1));

            TupleExpression ret = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t0, t1);
            assertEquals(new BigDecimal(2.0), ret.calculate(evaluatableTuple, filterCodeSystem));
            ret = new BinaryTupleExpression(ExpressionOperatorEnum.MINUS, t0, t1);
            assertEquals(new BigDecimal(0.0), ret.calculate(evaluatableTuple, filterCodeSystem));
            ret = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, t0, t1);
            assertEquals(new BigDecimal(1.0), ret.calculate(evaluatableTuple, filterCodeSystem));
            ret = new BinaryTupleExpression(ExpressionOperatorEnum.DIVIDE, t0, t1);
            assertEquals(new BigDecimal(1.0), ret.calculate(evaluatableTuple, filterCodeSystem));

            t1 = new ConstantTupleExpression(new BigDecimal(2.0));
            ret = new CaseTupleExpression(whenList, t1);

            PowerMockito.when(f0.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(true);
            assertEquals(new BigDecimal(1.0), ret.calculate(evaluatableTuple, filterCodeSystem));
            PowerMockito.when(f0.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(false);
            assertEquals(new BigDecimal(2.0), ret.calculate(evaluatableTuple, filterCodeSystem));
        }
    }

    /**
     * *           3                                                  10
     * *           |                                                   |
     * * 1*(1+2)*(col+1)*(3+4)+(1+2)*(3+1)*(4+5)-4+5 => 1*(1+2)*(col+1*n)*(3+4)+((1+2)*(3+1)*(4+5)-4+5)*n = 1363
     * <p>
     * *                                    +                                                     +
     * *                                 /     \                                          /               \
     * *                                -       5                                        -                *
     * *                           /         \                                   /               \       / \
     * *                          +           4                                 +                 *     n  5
     * *                 /                \                             /                \       / \
     * *                *                 *           ===>             *                 *      n  4
     * *            /       \          /     \                     /       \          /     \
     * *           *        +         *      +                    *        +         *      +
     * *        /     \    / \     /    \   / \                /     \    / \     /    \   / \
     * *       *      +   3  4    +     +  4  5               *      +   3  4    +     +  4  5
     * *      / \    / \         / \   / \                   / \    / \        /  \   / \
     * *     1  +  col 1        1  2  3  1                  1  +  col *       *   *  3  1
     * *       / \                                            / \    / \    / \  / \
     * *      1  2                                           1  2   n  1   n  1 n  2
     */
    @Test
    public void testDistribute1() {
        ConstantTupleExpression n = new ConstantTupleExpression(10);
        ExpressionCountDistributor cntDistributor = new ExpressionCountDistributor(n);

        TupleExpression t0 = new ConstantTupleExpression(1);
        TupleExpression t1 = new ConstantTupleExpression(1);
        TupleExpression t2 = new ConstantTupleExpression(2);

        TblColRef c = PowerMockito.mock(TblColRef.class);
        PowerMockito.when(c.getType()).thenReturn(DataType.getType("decimal"));
        TupleExpression t3 = new ColumnTupleExpression(c);
        IEvaluatableTuple evaluatableTuple = PowerMockito.mock(IEvaluatableTuple.class);
        IFilterCodeSystem filterCodeSystem = PowerMockito.mock(IFilterCodeSystem.class);
        t3 = PowerMockito.spy(t3);
        PowerMockito.when(t3.calculate(evaluatableTuple, filterCodeSystem)).thenReturn(new BigDecimal(3));

        TupleExpression t4 = new ConstantTupleExpression(1);
        TupleExpression t5 = new ConstantTupleExpression(3);
        TupleExpression t6 = new ConstantTupleExpression(4);
        TupleExpression t7 = new ConstantTupleExpression(1);
        TupleExpression t8 = new ConstantTupleExpression(2);
        TupleExpression t9 = new ConstantTupleExpression(3);
        TupleExpression t10 = new ConstantTupleExpression(1);
        TupleExpression t11 = new ConstantTupleExpression(4);
        TupleExpression t12 = new ConstantTupleExpression(5);
        TupleExpression t13 = new ConstantTupleExpression(4);
        TupleExpression t14 = new ConstantTupleExpression(5);

        TupleExpression b0 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t1, t2);

        TupleExpression b1 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, t0, b0);
        TupleExpression b2 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t3, t4);
        TupleExpression b3 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t7, t8);
        TupleExpression b4 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t9, t10);

        TupleExpression b11 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, b1, b2);
        TupleExpression b12 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t5, t6);
        TupleExpression b13 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, b3, b4);
        TupleExpression b14 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t11, t12);

        TupleExpression b21 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, b11, b12);
        TupleExpression b22 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, b13, b14);

        TupleExpression b31 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, b21, b22);

        TupleExpression b41 = new BinaryTupleExpression(ExpressionOperatorEnum.MINUS, b31, t13);

        TupleExpression b51 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, b41, t14);

        TupleExpression ret = b51.accept(cntDistributor);
        assertTrue(cntDistributor.ifCntSet());

        assertEquals(new BigDecimal(1363), ret.calculate(evaluatableTuple, filterCodeSystem));
    }

    /**
     * *                                          3                                                      10
     * *                                          |                                                       |
     * *  (1+2)*(case when f1 = 'c1' then (1+2)*(col+1)+3      (1+2)*(case when f1 = 'c1' then (1+2)*(col+n*1)+n*3
     * *              when f1 = 'c2' then (2+col)*(2+3)+4  =>              when f1 = 'c2' then (n*2+col)*(2+3)+n*4
     * *              else 6                                               else n*6
     * *         end) + col*2 + 1                                     end) + col*2 + n*1
     */
    @Test
    public void testDistribute2() {
        ConstantTupleExpression n = new ConstantTupleExpression(10);
        ExpressionCountDistributor cntDistributor = new ExpressionCountDistributor(n);

        TupleExpression t1 = new ConstantTupleExpression(1);
        TupleExpression t2 = new ConstantTupleExpression(2);

        TupleExpression t3 = new ConstantTupleExpression(1);
        TupleExpression t4 = new ConstantTupleExpression(2);

        TblColRef c = PowerMockito.mock(TblColRef.class);
        PowerMockito.when(c.getType()).thenReturn(DataType.getType("decimal"));
        TupleExpression t5 = new ColumnTupleExpression(c);
        IEvaluatableTuple evaluatableTuple = PowerMockito.mock(IEvaluatableTuple.class);
        IFilterCodeSystem filterCodeSystem = PowerMockito.mock(IFilterCodeSystem.class);
        t5 = PowerMockito.spy(t5);
        PowerMockito.when(t5.calculate(evaluatableTuple, filterCodeSystem)).thenReturn(new BigDecimal(3));

        TupleExpression t6 = new ConstantTupleExpression(1);
        TupleExpression t7 = new ConstantTupleExpression(3);

        TupleExpression t8 = new ConstantTupleExpression(2);
        TupleExpression t9 = new ConstantTupleExpression(2);
        TupleExpression t10 = new ConstantTupleExpression(3);
        TupleExpression t11 = new ConstantTupleExpression(4);

        TupleExpression t12 = new ConstantTupleExpression(6);

        TupleExpression t13 = new ConstantTupleExpression(2);
        TupleExpression t14 = new ConstantTupleExpression(1);

        TupleExpression b1 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t1, t2);
        TupleExpression b2 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t3, t4);
        TupleExpression b3 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t5, t6);
        TupleExpression b4 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t8, t5);
        TupleExpression b5 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, t9, t10);
        TupleExpression b6 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, t5, t13);

        TupleExpression b11 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, b2, b3);
        TupleExpression b12 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, b4, b5);

        TupleExpression b21 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, b11, t7);
        TupleExpression b22 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, b12, t11);

        TupleFilter f1 = PowerMockito.mock(CompareTupleFilter.class);
        TupleFilter f2 = PowerMockito.mock(CompareTupleFilter.class);

        List<Pair<TupleFilter, TupleExpression>> whenList = Lists.newArrayList();
        whenList.add(new Pair<>(f1, b21));
        whenList.add(new Pair<>(f2, b22));
        TupleExpression b31 = new CaseTupleExpression(whenList, t12);

        TupleExpression b41 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, b1, b31);

        TupleExpression b51 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, b41, b6);

        TupleExpression b61 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, b51, t14);

        TupleExpression ret = b61.accept(cntDistributor);
        assertTrue(cntDistributor.ifCntSet());

        PowerMockito.when(f1.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(true);
        assertEquals(new BigDecimal(223), ret.calculate(evaluatableTuple, filterCodeSystem));

        PowerMockito.when(f1.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(false);
        PowerMockito.when(f2.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(true);
        assertEquals(new BigDecimal(481), ret.calculate(evaluatableTuple, filterCodeSystem));

        PowerMockito.when(f1.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(false);
        PowerMockito.when(f2.evaluate(evaluatableTuple, filterCodeSystem)).thenReturn(false);
        assertEquals(new BigDecimal(196), ret.calculate(evaluatableTuple, filterCodeSystem));
    }
}