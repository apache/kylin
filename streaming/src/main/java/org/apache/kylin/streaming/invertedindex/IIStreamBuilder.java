/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.streaming.invertedindex;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.invertedindex.index.BatchSliceBuilder;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.invertedindex.model.IIRow;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.streaming.Stream;
import org.apache.kylin.streaming.StreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created by qianzhou on 3/3/15.
 */
public class IIStreamBuilder extends StreamBuilder {

    private static Logger logger = LoggerFactory.getLogger(IIStreamBuilder.class);

    private final IIDesc desc;
    private final HTableInterface hTable;
    private final BatchSliceBuilder sliceBuilder;

    public IIStreamBuilder(LinkedBlockingDeque<Stream> queue, String hTableName, IIDesc desc, int partitionId) {
        super(queue, desc.getSliceSize());
        this.desc = desc;
        try {
            this.hTable = HConnectionManager.createConnection(HBaseConfiguration.create()).getTable(hTableName);
        } catch (IOException e) {
            logger.error("cannot open htable name:" + hTableName, e);
            throw new RuntimeException("cannot open htable name:" + hTableName, e);
        }
        sliceBuilder = new BatchSliceBuilder(desc, (short) partitionId);
    }

    @Override
    protected void build(List<Stream> streamsToBuild) throws IOException {
        logger.info("stream build start, size:" + streamsToBuild.size());
        Stopwatch stopwatch = new Stopwatch().start();
        List<List<String>> table = Lists.transform(streamsToBuild, new Function<Stream, List<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable Stream input) {
                return parseStream(input, desc);
            }
        });
        final Map<Integer, Dictionary<?>> dictionaryMap = buildDictionary(table, desc);
        TableRecordInfo tableRecordInfo = new TableRecordInfo(desc, dictionaryMap);
        final Slice slice = buildSlice(table, sliceBuilder, tableRecordInfo, dictionaryMap);
        logger.info("slice info, shard:" + slice.getShard() + " timestamp:" + slice.getTimestamp() + " record count:" + slice.getRecordCount());
        loadToHBase(hTable, slice, new IIKeyValueCodec(tableRecordInfo.getDigest()));
        submitOffset();
        stopwatch.stop();
        logger.info("stream build finished, size:" + streamsToBuild.size() + " elapsed time:" + stopwatch.elapsedTime(TimeUnit.MILLISECONDS) + TimeUnit.MILLISECONDS);
    }

    private Map<Integer, Dictionary<?>> buildDictionary(List<List<String>> table, IIDesc desc) {
        HashMultimap<TblColRef, String> valueMap = HashMultimap.create();
        final List<TblColRef> allColumns = desc.listAllColumns();
        for (List<String> row : table) {
            for (int i = 0; i < row.size(); i++) {
                String cell = row.get(i);
                if (!desc.isMetricsCol(i)) {
                    valueMap.put(allColumns.get(i), cell);
                }
            }
        }
        Map<Integer, Dictionary<?>> result = Maps.newHashMap();
        for (TblColRef tblColRef : valueMap.keySet()) {
            result.put(desc.findColumn(tblColRef), DictionaryGenerator.buildDictionaryFromValueList(tblColRef.getType(), Collections2.transform(valueMap.get(tblColRef), new Function<String, byte[]>() {
                @Nullable
                @Override
                public byte[] apply(String input) {
                    return input.getBytes();
                }
            })));
        }
        return result;
    }

    private List<String> parseStream(Stream stream, IIDesc desc) {
        return getStreamParser().parse(stream, desc.listAllColumns());
    }

    private Slice buildSlice(List<List<String>> table, BatchSliceBuilder sliceBuilder, final TableRecordInfo tableRecordInfo, Map<Integer, Dictionary<?>> localDictionary) {
        final Slice slice = sliceBuilder.build(tableRecordInfo.getDigest(), Lists.transform(table, new Function<List<String>, TableRecord>() {
            @Nullable
            @Override
            public TableRecord apply(@Nullable List<String> input) {
                TableRecord result = tableRecordInfo.createTableRecord();
                for (int i = 0; i < input.size(); i++) {
                    result.setValueString(i, input.get(i));
                }
                return result;
            }
        }));
        slice.setLocalDictionaries(localDictionary);
        return slice;
    }

    private void loadToHBase(HTableInterface hTable, Slice slice, IIKeyValueCodec codec) throws IOException {
        try {
            List<Put> data = Lists.newArrayList();
            for (IIRow row : codec.encodeKeyValue(slice)) {
                final byte[] key = row.getKey().get();
                final byte[] value = row.getValue().get();
                Put put = new Put(key);
                put.add(IIDesc.HBASE_FAMILY_BYTES, IIDesc.HBASE_QUALIFIER_BYTES, value);
                final ImmutableBytesWritable dictionary = row.getDictionary();
                final byte[] dictBytes = dictionary.get();
                if (dictionary.getOffset() == 0 && dictionary.getLength() == dictBytes.length) {
                    put.add(IIDesc.HBASE_FAMILY_BYTES, IIDesc.HBASE_DICTIONARY_BYTES, dictBytes);
                } else {
                    throw new RuntimeException("dict offset should be 0, and dict length should be " + dictBytes.length + " but they are" + dictionary.getOffset() + " " + dictionary.getLength());
                }
                data.add(put);
            }
            hTable.put(data);
        } finally {
            hTable.close();
        }
    }


    private void submitOffset() {

    }

}
