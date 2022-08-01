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

package org.apache.kylin.metadata.model.tool;

import java.util.function.Function;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.ModelNonEquiCondMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class NonEquiJoinConditionComparatorTest {

    ModelNonEquiCondMock nonEquiMock = new ModelNonEquiCondMock();

    @After
    public void teardown() {
        nonEquiMock.clearTableRefCache();
    }

    @Test
    public void testSimpleEqual() {
        NonEquiJoinCondition cond1 = nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1");
        Assert.assertEquals(cond1, cond1);
    }

    @Test
    public void testNestedCondEqual() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.composite(SqlKind.AND, nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2"),
                        nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2")),
                nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a5", "B.b6")),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"), nonEquiMock.colConstantCompareCond(
                        SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR, "SOME TEXT", SqlTypeName.CHAR));
        Assert.assertEquals(cond1, cond1);
    }

    @Test
    public void testOrderingIrrelevantCondEqual() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"));
        NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"),
                nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"));
        Assert.assertEquals(cond1, cond2);
    }

    @Test
    public void testComparingWithNull() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"));
        Assert.assertNotEquals(cond1, null);
    }

    @Test
    public void testNotEqual() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.composite(SqlKind.AND, nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2"),
                        nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2")),
                nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a5", "B.b6")),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"), nonEquiMock.colConstantCompareCond(
                        SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR, "SOME TEXT", SqlTypeName.CHAR));
        NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.OR,
                nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a2", "B.b1"),
                nonEquiMock.composite(SqlKind.OR, nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b8"),
                        nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a3", "B.b2")),
                nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a5", "B.b6")),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a9", "B.b8"), nonEquiMock.colConstantCompareCond(
                        SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR, "SOME TEXT", SqlTypeName.CHAR));
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testNotEqualOnTable() {
        NonEquiJoinCondition cond1 = nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a1", "B.b1");
        NonEquiJoinCondition cond2 = nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a1", "C.b1");
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testNotEqualOnColumn() {
        NonEquiJoinCondition cond1 = nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a1", "B.b1");
        NonEquiJoinCondition cond2 = nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "A.a1", "B.b2");
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testNotEqualOnConstant() {
        NonEquiJoinCondition cond1 = nonEquiMock.colConstantCompareCond(SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR,
                "SOME TEXT", SqlTypeName.CHAR);
        NonEquiJoinCondition cond2 = nonEquiMock.colConstantCompareCond(SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR,
                "SOME TEXT1", SqlTypeName.CHAR);
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testNotEqualOnType() {
        NonEquiJoinCondition cond1 = nonEquiMock.colConstantCompareCond(SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR,
                "SOME TEXT", SqlTypeName.CHAR);
        NonEquiJoinCondition cond2 = nonEquiMock.colConstantCompareCond(SqlKind.EQUALS, "A.a9", SqlTypeName.CHAR,
                "SOME TEXT", SqlTypeName.INTEGER);
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testCondNormalizations() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.AND,
                nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a8", "B.b8"),
                nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a9", "B.b9")),
                nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colOp(SqlKind.IS_NULL, "A.a9")),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN_OR_EQUAL, "A.a1", "B.b1"),
                nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a4", "B.b4"));
        NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.AND,
                nonEquiMock.colCompareCond(SqlKind.EQUALS, "B.b8", "A.a8"),
                nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "B.b9", "A.a9"),
                nonEquiMock.colOp(SqlKind.IS_NOT_NULL, "A.a9"),
                nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1")),
                nonEquiMock.composite(SqlKind.NOT,
                        nonEquiMock.colCompareCond(SqlKind.GREATER_THAN_OR_EQUAL, "A.a4", "B.b4")));
        Assert.assertEquals(cond1, cond2);
    }

    @Test
    public void testCondNormalizationsOnSqlKindOTHER() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.EQUALS,
                nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "A.a9"),
                nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "B.a9"));
        NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.EQUALS,
                nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "B.a9"),
                nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "A.a9"));
        Assert.assertEquals(cond1, cond2);
    }

    @Test
    public void testCondNormalizationsOnSqlKindOTHERNotEqual() {
        NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.EQUALS,
                nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "A.a9"),
                nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "B.a9"));
        NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.EQUALS,
                nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "B.a9"),
                nonEquiMock.colOp("cos", SqlKind.OTHER_FUNCTION, "A.a9"));
        Assert.assertNotEquals(cond1, cond2);
    }

    @Test
    public void testNonEquJoinDescComparator() {

        Function<NonEquiJoinCondition, JoinDesc> supplierSimpleJoinDesc = (nonEquiJoinCondition) -> {
            JoinDesc joinDesc = new JoinDesc();
            joinDesc.setNonEquiJoinCondition(nonEquiJoinCondition);
            joinDesc.setPrimaryKey(new String[] {});
            joinDesc.setForeignKey(new String[] {});
            joinDesc.setForeignKeyColumns(new TblColRef[] {});
            joinDesc.setPrimaryKeyColumns(new TblColRef[] {});
            joinDesc.setType("LEFT");
            return joinDesc;
        };
        //nonEqual
        {
            NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.EQUALS,
                    nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "A.a9"),
                    nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "B.a9"));
            NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.EQUALS,
                    nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "B.a9"),
                    nonEquiMock.colOp("cos", SqlKind.OTHER_FUNCTION, "A.a9"));
            Assert.assertNotEquals(new JoinDescNonEquiCompBean(supplierSimpleJoinDesc.apply(cond1)),
                    new JoinDescNonEquiCompBean(supplierSimpleJoinDesc.apply(cond2)));
        }

        //equal
        {
            NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.EQUALS,
                    nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "A.a9"),
                    nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "B.a9"));
            NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.EQUALS,
                    nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "B.a9"),
                    nonEquiMock.colOp("abs", SqlKind.OTHER_FUNCTION, "A.a9"));
            Assert.assertEquals(new JoinDescNonEquiCompBean(supplierSimpleJoinDesc.apply(cond1)),
                    new JoinDescNonEquiCompBean(supplierSimpleJoinDesc.apply(cond2)));
        }
        //equal
        {
            NonEquiJoinCondition cond1 = nonEquiMock.composite(SqlKind.AND,
                    nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a8", "B.b8"),
                    nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colCompareCond(SqlKind.EQUALS, "A.a9", "B.b9")),
                    nonEquiMock.composite(SqlKind.NOT, nonEquiMock.colOp(SqlKind.IS_NULL, "A.a9")),
                    nonEquiMock.colCompareCond(SqlKind.LESS_THAN_OR_EQUAL, "A.a1", "B.b1"),
                    nonEquiMock.colCompareCond(SqlKind.LESS_THAN, "A.a4", "B.b4"));
            NonEquiJoinCondition cond2 = nonEquiMock.composite(SqlKind.AND,
                    nonEquiMock.colCompareCond(SqlKind.EQUALS, "B.b8", "A.a8"),
                    nonEquiMock.colCompareCond(SqlKind.NOT_EQUALS, "B.b9", "A.a9"),
                    nonEquiMock.colOp(SqlKind.IS_NOT_NULL, "A.a9"),
                    nonEquiMock.composite(SqlKind.NOT,
                            nonEquiMock.colCompareCond(SqlKind.GREATER_THAN, "A.a1", "B.b1")),
                    nonEquiMock.composite(SqlKind.NOT,
                            nonEquiMock.colCompareCond(SqlKind.GREATER_THAN_OR_EQUAL, "A.a4", "B.b4")));
            Assert.assertEquals(new JoinDescNonEquiCompBean(supplierSimpleJoinDesc.apply(cond1)),
                    new JoinDescNonEquiCompBean(supplierSimpleJoinDesc.apply(cond2)));

        }
    }

}
