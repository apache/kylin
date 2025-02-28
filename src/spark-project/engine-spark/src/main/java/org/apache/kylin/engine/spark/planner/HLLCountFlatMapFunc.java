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

package org.apache.kylin.engine.spark.planner;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;

import lombok.SneakyThrows;
import scala.Tuple2;

public class HLLCountFlatMapFunc implements PairFlatMapFunction<Iterator<Row>, BitSet, byte[]> {

    private final int precision;
    private final Map<BitSet, List<Integer>> cuboidIndexMap;

    public HLLCountFlatMapFunc(int precision, Map<BitSet, List<Integer>> cuboidIndexMap) {
        this.precision = precision;
        this.cuboidIndexMap = cuboidIndexMap;
    }

    @Override
    public Iterator<Tuple2<BitSet, byte[]>> call(Iterator<Row> rowIter) throws Exception {

        final HLLCountCounter hllCounter = new HLLCountCounter(precision, cuboidIndexMap);

        while (rowIter.hasNext()) {
            hllCounter.readRow(rowIter.next());
        }

        return new Iterator<Tuple2<BitSet, byte[]>>() {

            private final Iterator<TupleCountCounter> innerIter = hllCounter.counterIterator();

            @Override
            public boolean hasNext() {
                return innerIter.hasNext();
            }

            @SneakyThrows
            @Override
            public Tuple2<BitSet, byte[]> next() {
                TupleCountCounter counter = innerIter.next();
                return new Tuple2<>(counter.getCuboid(), counter.countBytes());
            }
        };
    }

}
