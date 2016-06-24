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

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 */
public class IteratorUtils {

    public static <K, V> Iterator<Tuple2<K, V>> merge(final Iterator<Tuple2<K, V>> input, final Comparator<K> comparator, final Function<Iterable<V>, V> converter) {
        return new Iterator<Tuple2<K, V>>() {

            Tuple2<K, V> current = input.hasNext() ? input.next() : null;

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Tuple2<K, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final LinkedList<V> values = Lists.newLinkedList();
                K currentKey = current._1();
                values.add(current._2());
                while (input.hasNext()) {
                    Tuple2<K, V> next = input.next();
                    if (comparator.compare(currentKey, next._1()) == 0) {
                        values.add(next._2());
                    } else {
                        current = next;
                        try {
                            return new Tuple2<>(currentKey, converter.call(values));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                if (!input.hasNext()) {
                    current = null;
                }
                try {
                    return new Tuple2<>(currentKey, converter.call(values));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
