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

package org.apache.kylin.measure.bitmap;

import org.junit.Assert;
import org.junit.Test;

public class BitmapBuildAggFuncTest {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;

    @Test
    public void testBitmapBuildAggFunc() {
        BitmapAggregator bitmapAggregator1 = BitmapBuildAggFunc.init();

        BitmapBuildAggFunc.add(bitmapAggregator1, 10L);
        BitmapCounter counter = factory.newBitmap(10L);
        Assert.assertEquals(counter, BitmapBuildAggFunc.result(bitmapAggregator1));

        BitmapBuildAggFunc.add(bitmapAggregator1, null);
        Assert.assertEquals(counter, BitmapBuildAggFunc.result(bitmapAggregator1));

        BitmapAggregator bitmapAggregator2 = BitmapBuildAggFunc.init();
        BitmapBuildAggFunc.add(bitmapAggregator2, 15L);
        BitmapBuildAggFunc.merge(bitmapAggregator1, bitmapAggregator2);
        Assert.assertEquals(counter, BitmapBuildAggFunc.result(bitmapAggregator1));

        BitmapBuildAggFunc.merge(bitmapAggregator1, null);
        Assert.assertEquals(counter, BitmapBuildAggFunc.result(bitmapAggregator1));
    }
}
