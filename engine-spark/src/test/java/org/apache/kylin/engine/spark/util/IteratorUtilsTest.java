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
package org.apache.kylin.engine.spark.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 */
public class IteratorUtilsTest {

    private static ArrayList<Tuple2<Integer, Integer>> getResult(List<Tuple2<Integer, Integer>> list) {

        return Lists.newArrayList(IteratorUtils.merge(list.iterator(), new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        }, new Function<Iterable<Integer>, Integer>() {
            @Override
            public Integer call(Iterable<Integer> v1) throws Exception {
                int sum = 0;
                for (Integer integer : v1) {
                    sum += integer;
                }
                return sum;
            }
        }));
    }

    @Test
    public void test() {
        List<Tuple2<Integer, Integer>> list = Lists.newArrayList();
        ArrayList<Tuple2<Integer, Integer>> result = null;

        result = getResult(list);
        assertTrue(result.size() == 0);
        System.out.println(result);

        list.clear();
        list.add(new Tuple2(0, 1));
        list.add(new Tuple2(0, 2));
        list.add(new Tuple2(1, 2));
        list.add(new Tuple2(1, 3));
        list.add(new Tuple2(2, 3));
        list.add(new Tuple2(2, 3));
        list.add(new Tuple2(3, 0));
        result = getResult(list);
        assertTrue(result.size() == 4);
        assertEquals(result.get(0), new Tuple2(0, 3));
        assertEquals(result.get(1), new Tuple2(1, 5));
        assertEquals(result.get(2), new Tuple2(2, 6));
        assertEquals(result.get(3), new Tuple2(3, 0));
        System.out.println(result);

        list.clear();
        list.add(new Tuple2(0, 1));
        result = getResult(list);
        assertTrue(result.size() == 1);
        System.out.println(result);
    }
}
