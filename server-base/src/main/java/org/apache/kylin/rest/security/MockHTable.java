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

/**
 * This file is licensed to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package org.apache.kylin.rest.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * MockHTable.
 *
 * original MockHTable (by agaoglu) : https://gist.github.com/agaoglu/613217#file_mock_h_table.java
 *
 * Modifications
 *
 * <ul>
 *     <li>fix filter (by k-mack) : https://gist.github.com/k-mack/4600133</li>
 *     <li>fix batch() : implement all mutate operation and fix result[] count.</li>
 *     <li>fix exists()</li>
 *     <li>fix increment() : wrong return value</li>
 *     <li>check columnFamily</li>
 *     <li>implement mutateRow()</li>
 *     <li>implement getTableName()</li>
 *     <li>implement getTableDescriptor()</li>
 *     <li>throws RuntimeException when unimplemented method was called.</li>
 *     <li>remove some methods for loading data, checking values ...</li>
 * </ul>
 */
public class MockHTable implements Table {
    private final String tableName;
    private final List<String> columnFamilies = new ArrayList<>();

    private NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> data = new TreeMap<>(Bytes.BYTES_COMPARATOR);

    private static List<KeyValue> toKeyValue(byte[] row, NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowdata, int maxVersions) {
        return toKeyValue(row, rowdata, 0, Long.MAX_VALUE, maxVersions);
    }

    public MockHTable(String tableName) {
        this.tableName = tableName;
    }

    public MockHTable(String tableName, String... columnFamilies) {
        this.tableName = tableName;
        this.columnFamilies.addAll(Arrays.asList(columnFamilies));
    }

    public void addColumnFamily(String columnFamily) {
        this.columnFamilies.add(columnFamily);
    }

    @Override
    public TableName getName() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getConfiguration() {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        HTableDescriptor table = new HTableDescriptor(tableName);
        for (String columnFamily : columnFamilies) {
            table.addFamily(new HColumnDescriptor(columnFamily));
        }
        return table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        // currently only support Put and Delete
        for (Mutation mutation : rm.getMutations()) {
            if (mutation instanceof Put) {
                put((Put) mutation);
            } else if (mutation instanceof Delete) {
                delete((Delete) mutation);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result append(Append append) throws IOException {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    private static List<KeyValue> toKeyValue(byte[] row, NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowdata, long timestampStart, long timestampEnd, int maxVersions) {
        List<KeyValue> ret = new ArrayList<KeyValue>();
        for (byte[] family : rowdata.keySet())
            for (byte[] qualifier : rowdata.get(family).keySet()) {
                int versionsAdded = 0;
                for (Map.Entry<Long, byte[]> tsToVal : rowdata.get(family).get(qualifier).descendingMap().entrySet()) {
                    if (versionsAdded++ == maxVersions)
                        break;
                    Long timestamp = tsToVal.getKey();
                    if (timestamp < timestampStart)
                        continue;
                    if (timestamp > timestampEnd)
                        continue;
                    byte[] value = tsToVal.getValue();
                    ret.add(new KeyValue(row, family, qualifier, timestamp, value));
                }
            }
        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(Get get) throws IOException {
        Result result = get(get);
        return result != null && result.isEmpty() == false;
    }

    @Override
    public boolean[] existsAll(List<Get> list) throws IOException {
        return new boolean[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
        results = batch(actions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
        Object[] results = new Object[actions.size()]; // same size.
        for (int i = 0; i < actions.size(); i++) {
            Row r = actions.get(i);
            if (r instanceof Delete) {
                delete((Delete) r);
                results[i] = new Result();
            }
            if (r instanceof Put) {
                put((Put) r);
                results[i] = new Result();
            }
            if (r instanceof Get) {
                Result result = get((Get) r);
                results[i] = result;
            }
            if (r instanceof Increment) {
                Result result = increment((Increment) r);
                results[i] = result;
            }
            if (r instanceof Append) {
                Result result = append((Append) r);
                results[i] = result;
            }
        }
        return results;
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) throws IOException, InterruptedException {

    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException, InterruptedException {
        return new Object[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result get(Get get) throws IOException {
        if (!data.containsKey(get.getRow()))
            return new Result();
        byte[] row = get.getRow();
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        if (!get.hasFamilies()) {
            kvs = toKeyValue(row, data.get(row), get.getMaxVersions());
        } else {
            for (byte[] family : get.getFamilyMap().keySet()) {
                if (data.get(row).get(family) == null)
                    continue;
                NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);
                if (qualifiers == null || qualifiers.isEmpty())
                    qualifiers = data.get(row).get(family).navigableKeySet();
                for (byte[] qualifier : qualifiers) {
                    if (qualifier == null)
                        qualifier = "".getBytes();
                    if (!data.get(row).containsKey(family) || !data.get(row).get(family).containsKey(qualifier) || data.get(row).get(family).get(qualifier).isEmpty())
                        continue;
                    Map.Entry<Long, byte[]> timestampAndValue = data.get(row).get(family).get(qualifier).lastEntry();
                    kvs.add(new KeyValue(row, family, qualifier, timestampAndValue.getKey(), timestampAndValue.getValue()));
                }
            }
        }
        Filter filter = get.getFilter();
        if (filter != null) {
            kvs = filter(filter, kvs);
        }

        return new Result(kvs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result[] get(List<Get> gets) throws IOException {
        List<Result> results = new ArrayList<Result>();
        for (Get g : gets) {
            results.add(get(g));
        }
        return results.toArray(new Result[results.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        final List<Result> ret = new ArrayList<Result>();
        byte[] st = scan.getStartRow();
        byte[] sp = scan.getStopRow();
        Filter filter = scan.getFilter();

        for (byte[] row : data.keySet()) {
            // if row is equal to startRow emit it. When startRow (inclusive) and
            // stopRow (exclusive) is the same, it should not be excluded which would
            // happen w/o this control.
            if (st != null && st.length > 0 && Bytes.BYTES_COMPARATOR.compare(st, row) != 0) {
                // if row is before startRow do not emit, pass to next row
                if (st != null && st.length > 0 && Bytes.BYTES_COMPARATOR.compare(st, row) > 0)
                    continue;
                // if row is equal to stopRow or after it do not emit, stop iteration
                if (sp != null && sp.length > 0 && Bytes.BYTES_COMPARATOR.compare(sp, row) <= 0)
                    break;
            }

            List<KeyValue> kvs = null;
            if (!scan.hasFamilies()) {
                kvs = toKeyValue(row, data.get(row), scan.getTimeRange().getMin(), scan.getTimeRange().getMax(), scan.getMaxVersions());
            } else {
                kvs = new ArrayList<KeyValue>();
                for (byte[] family : scan.getFamilyMap().keySet()) {
                    if (data.get(row).get(family) == null)
                        continue;
                    NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(family);
                    if (qualifiers == null || qualifiers.isEmpty())
                        qualifiers = data.get(row).get(family).navigableKeySet();
                    for (byte[] qualifier : qualifiers) {
                        if (data.get(row).get(family).get(qualifier) == null)
                            continue;
                        for (Long timestamp : data.get(row).get(family).get(qualifier).descendingKeySet()) {
                            if (timestamp < scan.getTimeRange().getMin())
                                continue;
                            if (timestamp > scan.getTimeRange().getMax())
                                continue;
                            byte[] value = data.get(row).get(family).get(qualifier).get(timestamp);
                            kvs.add(new KeyValue(row, family, qualifier, timestamp, value));
                            if (kvs.size() == scan.getMaxVersions()) {
                                break;
                            }
                        }
                    }
                }
            }
            if (filter != null) {
                kvs = filter(filter, kvs);
                // Check for early out optimization
                if (filter.filterAllRemaining()) {
                    break;
                }
            }
            if (!kvs.isEmpty()) {
                ret.add(new Result(kvs));
            }
        }

        return new ResultScanner() {
            private final Iterator<Result> iterator = ret.iterator();

            public Iterator<Result> iterator() {
                return iterator;
            }

            public Result[] next(int nbRows) throws IOException {
                ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
                for (int i = 0; i < nbRows; i++) {
                    Result next = next();
                    if (next != null) {
                        resultSets.add(next);
                    } else {
                        break;
                    }
                }
                return resultSets.toArray(new Result[resultSets.size()]);
            }

            public Result next() throws IOException {
                try {
                    return iterator().next();
                } catch (NoSuchElementException e) {
                    return null;
                }
            }

            public void close() {
            }
        };
    }

    /**
     * Follows the logical flow through the filter methods for a single row.
     *
     * @param filter HBase filter.
     * @param kvs    List of a row's KeyValues
     * @return List of KeyValues that were not filtered.
     */
    private List<KeyValue> filter(Filter filter, List<KeyValue> kvs) throws IOException {
        filter.reset();

        List<KeyValue> tmp = new ArrayList<KeyValue>(kvs.size());
        tmp.addAll(kvs);

        /*
         * Note. Filter flow for a single row. Adapted from
         * "HBase: The Definitive Guide" (p. 163) by Lars George, 2011.
         * See Figure 4-2 on p. 163.
         */
        boolean filteredOnRowKey = false;
        List<KeyValue> nkvs = new ArrayList<KeyValue>(tmp.size());
        for (KeyValue kv : tmp) {
            if (filter.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength())) {
                filteredOnRowKey = true;
                break;
            }
            Filter.ReturnCode filterResult = filter.filterKeyValue(kv);
            if (filterResult == Filter.ReturnCode.INCLUDE) {
                nkvs.add(kv);
            } else if (filterResult == Filter.ReturnCode.NEXT_ROW) {
                break;
            } else if (filterResult == Filter.ReturnCode.NEXT_COL || filterResult == Filter.ReturnCode.SKIP) {
                continue;
            }
            /*
             * Ignoring next key hint which is a optimization to reduce file
             * system IO
             */
        }
        if (filter.hasFilterRow() && !filteredOnRowKey) {
            filter.filterRow();
        }
        if (filter.filterRow() || filteredOnRowKey) {
            nkvs.clear();
        }
        tmp = nkvs;
        return tmp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(scan);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(scan);
    }

    private <K, V> V forceFind(NavigableMap<K, V> map, K key, V newObject) {
        V data = map.get(key);
        if (data == null) {
            data = newObject;
            map.put(key, data);
        }
        return data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(Put put) throws IOException {
        byte[] row = put.getRow();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = forceFind(data, row, new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>(Bytes.BYTES_COMPARATOR));
        for (byte[] family : put.getFamilyMap().keySet()) {
            if (columnFamilies.contains(new String(family)) == false) {
                throw new RuntimeException("Not Exists columnFamily : " + new String(family));
            }
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = forceFind(rowData, family, new TreeMap<byte[], NavigableMap<Long, byte[]>>(Bytes.BYTES_COMPARATOR));
            for (KeyValue kv : put.getFamilyMap().get(family)) {
                kv.updateLatestStamp(Bytes.toBytes(System.currentTimeMillis()));
                byte[] qualifier = kv.getQualifier();
                NavigableMap<Long, byte[]> qualifierData = forceFind(familyData, qualifier, new TreeMap<Long, byte[]>());
                qualifierData.put(kv.getTimestamp(), kv.getValue());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(List<Put> puts) throws IOException {
        for (Put put : puts) {
            put(put);
        }

    }

    private boolean check(byte[] row, byte[] family, byte[] qualifier, byte[] value) {
        if (value == null || value.length == 0)
            return !data.containsKey(row) || !data.get(row).containsKey(family) || !data.get(row).get(family).containsKey(qualifier);
        else
            return data.containsKey(row) && data.get(row).containsKey(family) && data.get(row).get(family).containsKey(qualifier) && !data.get(row).get(family).get(qualifier).isEmpty() && Arrays.equals(data.get(row).get(family).get(qualifier).lastEntry().getValue(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        if (check(row, family, qualifier, value)) {
            put(put);
            return true;
        }
        return false;
    }

    @Override
    public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, Put put) throws IOException {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(Delete delete) throws IOException {
        byte[] row = delete.getRow();
        if (data.get(row) == null)
            return;
        if (delete.getFamilyMap().size() == 0) {
            data.remove(row);
            return;
        }
        for (byte[] family : delete.getFamilyMap().keySet()) {
            if (data.get(row).get(family) == null)
                continue;
            if (delete.getFamilyMap().get(family).isEmpty()) {
                data.get(row).remove(family);
                continue;
            }
            for (KeyValue kv : delete.getFamilyMap().get(family)) {
                if (kv.isDelete()) {
                    data.get(row).get(kv.getFamily()).clear();
                } else {
                    data.get(row).get(kv.getFamily()).remove(kv.getQualifier());
                }
            }
            if (data.get(row).get(family).isEmpty()) {
                data.get(row).remove(family);
            }
        }
        if (data.get(row).isEmpty()) {
            data.remove(row);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(List<Delete> deletes) throws IOException {
        for (Delete delete : deletes) {
            delete(delete);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
        if (check(row, family, qualifier, value)) {
            delete(delete);
            return true;
        }
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, Delete delete) throws IOException {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result increment(Increment increment) throws IOException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        return incrementColumnValue(row, family, qualifier, amount, null);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        throw new NotImplementedException();

    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws ServiceException, Throwable {
        throw new NotImplementedException();

    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback) throws ServiceException, Throwable {
        throw new NotImplementedException();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getWriteBufferSize() {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        throw new NotImplementedException();

    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
        throw new NotImplementedException();

    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype, Batch.Callback<R> callback) throws ServiceException, Throwable {
        throw new NotImplementedException();

    }

    //@Override  (only since 0.98.8)
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations mutation) throws IOException {
        throw new NotImplementedException();

    }
}