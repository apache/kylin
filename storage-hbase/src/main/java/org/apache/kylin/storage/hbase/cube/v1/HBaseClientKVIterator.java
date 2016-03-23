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

package org.apache.kylin.storage.hbase.cube.v1;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIRow;

/**
 * @author yangli9
 * 
 */
public class HBaseClientKVIterator implements Iterable<IIRow>, Closeable {

    byte[] family;

    Table table;
    ResultScanner scanner;
    Iterator<Result> iterator;

    public HBaseClientKVIterator(Connection hconn, String tableName, byte[] family) throws IOException {
        this.family = family;

        this.table = hconn.getTable(TableName.valueOf(tableName));
        this.scanner = table.getScanner(family);
        this.iterator = scanner.iterator();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(scanner);
        IOUtils.closeQuietly(table);
    }

    @Override
    public Iterator<IIRow> iterator() {
        return new MyIterator();
    }

    private class MyIterator implements Iterator<IIRow> {

        ImmutableBytesWritable key = new ImmutableBytesWritable();
        ImmutableBytesWritable value = new ImmutableBytesWritable();
        ImmutableBytesWritable dict = new ImmutableBytesWritable();
        IIRow pair = new IIRow(key, value, dict);

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public IIRow next() {
            Result r = iterator.next();
            Cell c = r.getColumnLatestCell(IIDesc.HBASE_FAMILY_BYTES, IIDesc.HBASE_QUALIFIER_BYTES);
            key.set(c.getRowArray(), c.getRowOffset(), c.getRowLength());
            value.set(c.getValueArray(), c.getValueOffset(), c.getValueLength());
            c = r.getColumnLatestCell(IIDesc.HBASE_FAMILY_BYTES, IIDesc.HBASE_DICTIONARY_BYTES);
            dict.set(c.getValueArray(), c.getValueOffset(), c.getValueLength());
            return pair;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
