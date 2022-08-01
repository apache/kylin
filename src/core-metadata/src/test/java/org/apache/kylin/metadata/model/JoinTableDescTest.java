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

package org.apache.kylin.metadata.model;

import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JoinTableDescTest {

    private JoinTableDesc first;
    private JoinTableDesc second;

    @Before
    public void setUp() {
        first = new JoinTableDesc();
        JoinDesc join = new JoinDesc();
        join.setForeignKey(new String[] { "a.col" });
        join.setPrimaryKey(new String[] { "b.col" });
        join.setType("inner");
        join.setPrimaryTable("s.b");
        join.setForeignTable("s.a");
        first.setJoin(join);

        second = new JoinTableDesc();
        JoinDesc join2 = new JoinDesc();
        join2.setForeignKey(new String[] { "a.col" });
        join2.setPrimaryKey(new String[] { "b.col" });
        join2.setType("inner");
        join2.setPrimaryTable("s.b");
        join2.setForeignTable("s.a");
        second.setJoin(join2);
    }

    @Test
    public void basicTest() {

        Assert.assertTrue(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());

        first.setFlattenable(JoinTableDesc.FLATTEN);
        Assert.assertTrue(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());

        first.setFlattenable("other");
        Assert.assertTrue(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        Assert.assertFalse(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        first.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY);
        Assert.assertFalse(first.isFlattenable());
        Assert.assertFalse(first.isDerivedForbidden());
        Assert.assertTrue(first.isDerivedToManyJoinRelation());

        first.setFlattenable(JoinTableDesc.FLATTEN);
        first.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY);
        Assert.assertTrue(first.isFlattenable());
        Assert.assertTrue(first.isDerivedForbidden());
        Assert.assertFalse(first.isDerivedToManyJoinRelation());
    }

    @Test
    public void testHasDifferentAntiFlattenable() {

        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.FLATTEN);
        second.setFlattenable(null);
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(null);
        second.setFlattenable(JoinTableDesc.FLATTEN);
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.FLATTEN);
        second.setFlattenable(JoinTableDesc.FLATTEN);
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        second.setFlattenable(JoinTableDesc.NORMALIZED);
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        second.setFlattenable(JoinTableDesc.FLATTEN);
        Assert.assertTrue(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        second.setFlattenable(null);
        Assert.assertTrue(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(null);
        second.setFlattenable(JoinTableDesc.NORMALIZED);
        Assert.assertTrue(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(null);
        second.setFlattenable("other");
        Assert.assertFalse(first.hasDifferentAntiFlattenable(second));

        first.setFlattenable(JoinTableDesc.NORMALIZED);
        second.setFlattenable("other");
        Assert.assertTrue(first.hasDifferentAntiFlattenable(second));
    }
}
