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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kylin.engine.spark.SparkCuboidWriter;
import org.apache.kylin.gridtable.GTRecord;

import scala.Tuple2;

/**
 */
public class BufferedCuboidWriter implements SparkCuboidWriter {

    private final LinkedBlockingQueue<Tuple2<byte[], byte[]>> blockingQueue;
    private final TupleConverter tupleConverter;

    public BufferedCuboidWriter(TupleConverter tupleConverter) {
        this.blockingQueue = new LinkedBlockingQueue<>(10000);
        this.tupleConverter = tupleConverter;
    }

    @Override
    public void write(final long cuboidId, final GTRecord record) throws IOException {
        try {
            blockingQueue.put(tupleConverter.convert(cuboidId, record));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
        try {
            blockingQueue.put(new Tuple2(new byte[0], new byte[0]));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<Tuple2<byte[], byte[]>> getResult() {
        return new Iterable<Tuple2<byte[], byte[]>>() {
            @Override
            public Iterator<Tuple2<byte[], byte[]>> iterator() {
                return new Iterator<Tuple2<byte[], byte[]>>() {
                    Tuple2<byte[], byte[]> current = null;

                    @Override
                    public boolean hasNext() {
                        if (current == null) {
                            try {
                                current = blockingQueue.take();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        }
                        return current._1().length > 0 && current._2().length > 0;
                    }

                    @Override
                    public Tuple2<byte[], byte[]> next() {
                        if (hasNext()) {
                            Tuple2<byte[], byte[]> result = current;
                            current = null;
                            return result;
                        } else {
                            throw new NoSuchElementException();
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
