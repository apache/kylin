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

package org.apache.kylin.newten;

import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.udaf.BitmapSerAndDeSer;
import org.apache.spark.sql.udf.SubtractBitmapImpl;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class SubtractBitmapImplTest {

    @Test
    public void testSubtractBimapImplNullNull() {
        Assert.assertNull(SubtractBitmapImpl.evaluate2Bytes(null, null));
        Assert.assertNull(SubtractBitmapImpl.evaluate2AllValues(null, null, 10000000));
        Assert.assertEquals(0, SubtractBitmapImpl.evaluate2Count(null, null));
        Assert.assertNull(SubtractBitmapImpl.evaluate2Values(null, null, 1, 0, 10000000));
    }

    @Test
    public void testSubtractBimapImplMap2Null() {
        Roaring64NavigableMap roaring64NavigableMap1 = new Roaring64NavigableMap();
        for (long i = 0; i < 10; i++) {
            roaring64NavigableMap1.add(i);
        }
        byte[] map1 = BitmapSerAndDeSer.get().serialize(roaring64NavigableMap1);
        byte[] evaluate2Bytes = SubtractBitmapImpl.evaluate2Bytes(map1, null);
        Assert.assertEquals(map1.length, evaluate2Bytes.length);
        for (int i = 0; i < map1.length; i++) {
            Assert.assertEquals(map1[i], evaluate2Bytes[i]);
        }

        int evaluate2Count = SubtractBitmapImpl.evaluate2Count(map1, null);
        Assert.assertEquals(roaring64NavigableMap1.getIntCardinality(), evaluate2Count);

        int bitmapUpperBound = 10000000;
        GenericArrayData genericArrayDataAll = SubtractBitmapImpl.evaluate2AllValues(map1, null, bitmapUpperBound);
        Assert.assertEquals(roaring64NavigableMap1.getIntCardinality(), genericArrayDataAll.numElements());
        Roaring64NavigableMap evaluate2AllValues = new Roaring64NavigableMap();
        for (Object o : genericArrayDataAll.array()) {
            evaluate2AllValues.add(((long) o));
        }
        evaluate2AllValues.andNot(roaring64NavigableMap1);
        Assert.assertEquals(0, evaluate2AllValues.getIntCardinality());

        GenericArrayData genericArrayData = SubtractBitmapImpl.evaluate2Values(map1, null, 5, 0, bitmapUpperBound);
        Assert.assertEquals(5, genericArrayData.numElements());
        Roaring64NavigableMap evaluate2Values = new Roaring64NavigableMap();
        for (Object o : genericArrayData.array()) {
            evaluate2Values.add(((long) o));
        }
        roaring64NavigableMap1.andNot(evaluate2Values);
        Assert.assertEquals(5, roaring64NavigableMap1.getIntCardinality());
        Roaring64NavigableMap result = new Roaring64NavigableMap();
        for (long i = 5; i < 10; i++) {
            result.add(i);
        }
        Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();
        roaring64NavigableMap.or(roaring64NavigableMap1);
        roaring64NavigableMap.andNot(result);
        Assert.assertEquals(0, roaring64NavigableMap.getIntCardinality());

        genericArrayData = SubtractBitmapImpl.evaluate2Values(map1, null, 0, 0, 10);
        Assert.assertEquals(0, genericArrayData.numElements());

        genericArrayData = SubtractBitmapImpl.evaluate2Values(map1, null, 0, 11, 10);
        Assert.assertEquals(0, genericArrayData.numElements());

        genericArrayData = SubtractBitmapImpl.evaluate2Values(map1, null, 11, 11, 10);
        Assert.assertEquals(0, genericArrayData.numElements());
    }

    @Test
    public void testSubtractBimapImplError() {
        Roaring64NavigableMap roaring64NavigableMap1 = new Roaring64NavigableMap();
        for (long i = 0; i < 10; i++) {
            roaring64NavigableMap1.add(i);
        }
        byte[] map1 = BitmapSerAndDeSer.get().serialize(roaring64NavigableMap1);
        try {
            SubtractBitmapImpl.evaluate2Values(map1, null, -1, 0, 1000);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
            Assert.assertEquals("both limit and offset must be >= 0", e.getMessage());
        }

        try {
            SubtractBitmapImpl.evaluate2Values(map1, null, 1, -1, 1000);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
            Assert.assertEquals("both limit and offset must be >= 0", e.getMessage());
        }

        try {
            SubtractBitmapImpl.evaluate2Values(map1, null, 1, 10, 1);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
            Assert.assertEquals("Cardinality of the bitmap is greater than configured upper bound(1).", e.getMessage());
        }

        try {
            SubtractBitmapImpl.evaluate2AllValues(map1, null, 1);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
            Assert.assertEquals("Cardinality of the bitmap is greater than configured upper bound(1).", e.getMessage());
        }
    }

    @Test
    public void testSubtractBimapImpl() {
        Roaring64NavigableMap roaring64NavigableMap1 = new Roaring64NavigableMap();
        for (long i = 0; i < 10; i++) {
            roaring64NavigableMap1.add(i);
        }
        byte[] map1 = BitmapSerAndDeSer.get().serialize(roaring64NavigableMap1);
        Roaring64NavigableMap roaring64NavigableMap2 = new Roaring64NavigableMap();
        for (long i = 0; i < 5; i++) {
            roaring64NavigableMap2.add(i);
        }
        byte[] map2 = BitmapSerAndDeSer.get().serialize(roaring64NavigableMap2);

        GenericArrayData genericArrayData = SubtractBitmapImpl.evaluate2AllValues(map1, map2, 1000);
        Assert.assertEquals(5, genericArrayData.numElements());
        Roaring64NavigableMap evaluate2Values = new Roaring64NavigableMap();
        for (Object o : genericArrayData.array()) {
            evaluate2Values.add(((long) o));
        }
        Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();
        roaring64NavigableMap.or(roaring64NavigableMap1);
        roaring64NavigableMap.andNot(evaluate2Values);
        Assert.assertEquals(5, roaring64NavigableMap.getIntCardinality());
        roaring64NavigableMap.andNot(roaring64NavigableMap2);
        Assert.assertEquals(0, roaring64NavigableMap.getIntCardinality());

        int evaluate2Count = SubtractBitmapImpl.evaluate2Count(map1, map2);
        Assert.assertEquals(5, evaluate2Count);

        byte[] evaluate2Bytes = SubtractBitmapImpl.evaluate2Bytes(map1, map2);
        roaring64NavigableMap = new Roaring64NavigableMap();
        roaring64NavigableMap.or(roaring64NavigableMap1);
        roaring64NavigableMap.andNot(roaring64NavigableMap2);
        byte[] map = BitmapSerAndDeSer.get().serialize(roaring64NavigableMap);
        Assert.assertEquals(map.length, evaluate2Bytes.length);
        for (int i = 0; i < map.length; i++) {
            Assert.assertEquals(map[i], evaluate2Bytes[i]);
        }

        genericArrayData = SubtractBitmapImpl.evaluate2Values(map1, map2, 10, 0, 1000);
        Assert.assertEquals(5, genericArrayData.numElements());
        evaluate2Values = new Roaring64NavigableMap();
        for (Object o : genericArrayData.array()) {
            evaluate2Values.add(((long) o));
        }
        roaring64NavigableMap = new Roaring64NavigableMap();
        roaring64NavigableMap.or(roaring64NavigableMap1);
        roaring64NavigableMap.andNot(evaluate2Values);
        Assert.assertEquals(5, roaring64NavigableMap.getIntCardinality());
        roaring64NavigableMap.andNot(roaring64NavigableMap2);
        Assert.assertEquals(0, roaring64NavigableMap.getIntCardinality());
    }
}
