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
import org.apache.kylin.metadata.expression.TupleExpression.ExpressionOperatorEnum;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class ExpressionCountDistributorTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
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
        NumberTupleExpression n = new NumberTupleExpression(10);
        ExpressionCountDistributor cntDistributor = new ExpressionCountDistributor(n);

        TupleExpression t0 = new NumberTupleExpression(1);
        TupleExpression t1 = new NumberTupleExpression(1);
        TupleExpression t2 = new NumberTupleExpression(2);

        TblColRef c = PowerMockito.mock(TblColRef.class);
        TupleExpression t3 = new ColumnTupleExpression(c);
        IEvaluatableTuple evaluatableTuple = PowerMockito.mock(IEvaluatableTuple.class);
        IFilterCodeSystem filterCodeSystem = PowerMockito.mock(IFilterCodeSystem.class);
        t3 = PowerMockito.spy(t3);
        PowerMockito.when(t3.calculate(evaluatableTuple, filterCodeSystem)).thenReturn(new BigDecimal(3));

        TupleExpression t4 = new NumberTupleExpression(1);
        TupleExpression t5 = new NumberTupleExpression(3);
        TupleExpression t6 = new NumberTupleExpression(4);
        TupleExpression t7 = new NumberTupleExpression(1);
        TupleExpression t8 = new NumberTupleExpression(2);
        TupleExpression t9 = new NumberTupleExpression(3);
        TupleExpression t10 = new NumberTupleExpression(1);
        TupleExpression t11 = new NumberTupleExpression(4);
        TupleExpression t12 = new NumberTupleExpression(5);
        TupleExpression t13 = new NumberTupleExpression(4);
        TupleExpression t14 = new NumberTupleExpression(5);

        TupleExpression b0 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t1, t2));

        TupleExpression b1 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(t0, b0));
        TupleExpression b2 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t3, t4));
        TupleExpression b3 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t7, t8));
        TupleExpression b4 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t9, t10));

        TupleExpression b11 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(b1, b2));
        TupleExpression b12 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t5, t6));
        TupleExpression b13 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(b3, b4));
        TupleExpression b14 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t11, t12));

        TupleExpression b21 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(b11, b12));
        TupleExpression b22 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(b13, b14));

        TupleExpression b31 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(b21, b22));

        TupleExpression b41 = new BinaryTupleExpression(ExpressionOperatorEnum.MINUS, Lists.newArrayList(b31, t13));

        TupleExpression b51 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(b41, t14));

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
        NumberTupleExpression n = new NumberTupleExpression(10);
        ExpressionCountDistributor cntDistributor = new ExpressionCountDistributor(n);

        TupleExpression t1 = new NumberTupleExpression(1);
        TupleExpression t2 = new NumberTupleExpression(2);

        TupleExpression t3 = new NumberTupleExpression(1);
        TupleExpression t4 = new NumberTupleExpression(2);

        TblColRef c = PowerMockito.mock(TblColRef.class);
        TupleExpression t5 = new ColumnTupleExpression(c);
        IEvaluatableTuple evaluatableTuple = PowerMockito.mock(IEvaluatableTuple.class);
        IFilterCodeSystem filterCodeSystem = PowerMockito.mock(IFilterCodeSystem.class);
        t5 = PowerMockito.spy(t5);
        PowerMockito.when(t5.calculate(evaluatableTuple, filterCodeSystem)).thenReturn(new BigDecimal(3));

        TupleExpression t6 = new NumberTupleExpression(1);
        TupleExpression t7 = new NumberTupleExpression(3);

        TupleExpression t8 = new NumberTupleExpression(2);
        TupleExpression t9 = new NumberTupleExpression(2);
        TupleExpression t10 = new NumberTupleExpression(3);
        TupleExpression t11 = new NumberTupleExpression(4);

        TupleExpression t12 = new NumberTupleExpression(6);

        TupleExpression t13 = new NumberTupleExpression(2);
        TupleExpression t14 = new NumberTupleExpression(1);

        TupleExpression b1 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t1, t2));
        TupleExpression b2 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t3, t4));
        TupleExpression b3 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t5, t6));
        TupleExpression b4 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t8, t5));
        TupleExpression b5 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(t9, t10));
        TupleExpression b6 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(t5, t13));

        TupleExpression b11 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(b2, b3));
        TupleExpression b12 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(b4, b5));

        TupleExpression b21 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(b11, t7));
        TupleExpression b22 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(b12, t11));

        TupleFilter f1 = PowerMockito.mock(CompareTupleFilter.class);
        TupleFilter f2 = PowerMockito.mock(CompareTupleFilter.class);

        List<Pair<TupleFilter, TupleExpression>> whenList = Lists.newArrayList();
        whenList.add(new Pair<>(f1, b21));
        whenList.add(new Pair<>(f2, b22));
        TupleExpression b31 = new CaseTupleExpression(whenList, t12);

        TupleExpression b41 = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE, Lists.newArrayList(b1, b31));

        TupleExpression b51 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(b41, b6));

        TupleExpression b61 = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS, Lists.newArrayList(b51, t14));

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
