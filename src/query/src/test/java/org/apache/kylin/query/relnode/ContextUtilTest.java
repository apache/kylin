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

package org.apache.kylin.query.relnode;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.util.Unsafe;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ContextUtilTest {

    //https://github.com/Kyligence/KAP/issues/9952
    //do not support agg pushdown if WindowRel, SortRel, LimitRel, ValueRel is met
    @Test
    public void testDerivedFromSameContextWhenMetWindowOrSort() throws Exception {
        Method derivedMethod = ContextUtil.class.getDeclaredMethod("derivedFromSameContext", Collection.class,
                RelNode.class, OLAPContext.class, boolean.class);
        Unsafe.changeAccessibleObject(derivedMethod, true);
        {
            RelNode rel = Mockito.mock(OLAPWindowRel.class);
            Object result = derivedMethod.invoke(null, Collections.EMPTY_LIST, rel, null, false);
            Assert.assertEquals(false, result);
        }
        {
            RelNode rel = Mockito.mock(OLAPSortRel.class);
            Object result = derivedMethod.invoke(null, Collections.EMPTY_LIST, rel, null, false);
            Assert.assertEquals(false, result);
        }
        {
            RelNode rel = Mockito.mock(OLAPLimitRel.class);
            Object result = derivedMethod.invoke(null, Collections.EMPTY_LIST, rel, null, false);
            Assert.assertEquals(false, result);
        }
        {
            RelNode rel = Mockito.mock(OLAPValuesRel.class);
            Object result = derivedMethod.invoke(null, Collections.EMPTY_LIST, rel, null, false);
            Assert.assertEquals(false, result);
        }
    }
}
