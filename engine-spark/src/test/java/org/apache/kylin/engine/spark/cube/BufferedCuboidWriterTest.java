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
package org.apache.kylin.engine.spark.cube;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kylin.gridtable.GTRecord;
import org.junit.Test;

import scala.Tuple2;

/**
 */
public class BufferedCuboidWriterTest {

    @Test
    public void test() throws ExecutionException, InterruptedException {
        final BufferedCuboidWriter bufferedCuboidWriter = new BufferedCuboidWriter(new TupleConverter() {
            @Override
            public Tuple2<byte[], byte[]> convert(long cuboidId, GTRecord record) {
                return new Tuple2<>(Long.valueOf(cuboidId).toString().getBytes(), Long.valueOf(cuboidId).toString().getBytes());
            }
        });
        final int testCount = 10000000;
        final Future<?> future = Executors.newCachedThreadPool().submit(new Runnable() {
            @Override
            public void run() {
                int i = 0;

                while (i++ < testCount) {
                    try {
                        bufferedCuboidWriter.write(i, null);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
                bufferedCuboidWriter.close();
            }
        });
        long actualCount = 0;
        for (Tuple2<byte[], byte[]> tuple2 : bufferedCuboidWriter.getResult()) {
            assertEquals(Long.parseLong(new String(tuple2._1())), ++actualCount);
        }
        future.get();
        assertEquals(actualCount, testCount);

    }
}
