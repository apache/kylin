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

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.kylin.invertedindex.model.KeyValuePair;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by honma on 11/10/14.
 */
public class HbaseServerKVIterator implements Iterable<KeyValuePair>, Closeable {

    private RegionScanner innerScanner;

    List<Cell> results = new ArrayList<Cell>();

    public HbaseServerKVIterator(RegionScanner innerScanner) {
        this.innerScanner = innerScanner;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.innerScanner);
    }

    @Override
    public Iterator<KeyValuePair> iterator() {
        return new Iterator<KeyValuePair>() {

            ImmutableBytesWritable key = new ImmutableBytesWritable();
            ImmutableBytesWritable value = new ImmutableBytesWritable();
            KeyValuePair pair = new KeyValuePair(key, value);

            private boolean hasMore = true;

            @Override
            public boolean hasNext() {
                return hasMore;
            }

            @Override
            public KeyValuePair next() {
                if (hasNext()) {
                    try {
                        hasMore = innerScanner.nextRaw(results);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    if (results.size() < 1)
                        throw new IllegalStateException("Hbase row contains less than 1 cell");

                    Cell c = results.get(0);
                    key.set(c.getRowArray(), c.getRowOffset(), c.getRowLength());
                    value.set(c.getValueArray(), c.getValueOffset(), c.getValueLength());

                    results.clear();
                    return pair;
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
