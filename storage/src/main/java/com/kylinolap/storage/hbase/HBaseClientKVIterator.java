/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;

import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;

/**
 * @author yangli9
 * 
 */
public class HBaseClientKVIterator implements Iterable<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>, Closeable {

    byte[] family;
    byte[] qualifier;

    HTableInterface table;
    ResultScanner scanner;
    Iterator<Result> iterator;

    public HBaseClientKVIterator(HConnection hconn, String tableName, byte[] family, byte[] qualifier) throws IOException {
        this.family = family;
        this.qualifier = qualifier;

        this.table = hconn.getTable(tableName);
        this.scanner = table.getScanner(family, qualifier);
        this.iterator = scanner.iterator();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(scanner);
        IOUtils.closeQuietly(table);
    }

    @Override
    public Iterator<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> iterator() {
        return new MyIterator();
    }

    private class MyIterator implements Iterator<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> {

        ImmutableBytesWritable key = new ImmutableBytesWritable();
        ImmutableBytesWritable value = new ImmutableBytesWritable();
        Pair<ImmutableBytesWritable, ImmutableBytesWritable> pair = new Pair<ImmutableBytesWritable, ImmutableBytesWritable>(key, value);

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Pair<ImmutableBytesWritable, ImmutableBytesWritable> next() {
            Result r = iterator.next();
            Cell c = r.getColumnLatestCell(InvertedIndexDesc.HBASE_FAMILY_BYTES, InvertedIndexDesc.HBASE_QUALIFIER_BYTES);
            key.set(c.getRowArray(), c.getRowOffset(), c.getRowLength());
            value.set(c.getValueArray(), c.getValueOffset(), c.getValueLength());
            return pair;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
