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
package org.apache.kylin.measure.sumlc;

import org.junit.Assert;
import org.junit.Test;

public class SumLCCounterTest {
    private static final Long PAST_VAL = 1L;
    private static final Long PAST_TS = 915120000000L;
    private static final Long LATER_VAL = 2L;
    private static final Long LATER_TS = 1640966400000L;

    @Test
    public void testSumLCUpdate() {
        SumLCCounter target = new SumLCCounter();

        target.update(PAST_VAL, PAST_TS);
        Assert.assertEquals(PAST_VAL, target.getSumLC());
        Assert.assertEquals(PAST_TS, target.getTimestamp());

        target.update(null, LATER_TS);
        Assert.assertEquals(null, target.getSumLC());
        Assert.assertEquals(LATER_TS, target.getTimestamp());

        target.update(null, null);
        Assert.assertEquals(null, target.getSumLC());
        Assert.assertEquals(LATER_TS, target.getTimestamp());

        target.update(LATER_VAL, LATER_TS);
        Assert.assertEquals(LATER_VAL, target.getSumLC());
        Assert.assertEquals(LATER_TS, target.getTimestamp());

        target.update(PAST_VAL + LATER_VAL, LATER_TS);
        Assert.assertEquals(PAST_VAL + LATER_VAL + LATER_VAL, target.getSumLC());
        Assert.assertEquals(LATER_TS, target.getTimestamp());
    }

}
