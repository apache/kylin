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
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.invertedindex.index.BatchSliceBuilder;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.streaming.Stream;
import org.apache.kylin.streaming.StreamParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by qianzhou on 3/27/15.
 */
public final class SliceBuilder {

    private static Logger logger = LoggerFactory.getLogger(SliceBuilder.class);

    public SliceBuilder(IIDesc desc, short shard) {
        this.iiDesc = desc;
        this.sliceBuilder = new BatchSliceBuilder(desc, shard);
    }

    private final BatchSliceBuilder sliceBuilder;
    private final IIDesc iiDesc;

    public Slice buildSlice(List<Stream> streams, final StreamParser streamParser) {
        List<List<String>> table = Lists.transform(streams, new Function<Stream, List<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable Stream input) {
                return streamParser.parse(input);
            }
        });
        final Dictionary<?>[] dictionaryMap = buildDictionary(table, iiDesc);
        TableRecordInfo tableRecordInfo = new TableRecordInfo(iiDesc, dictionaryMap);
        return build(table, sliceBuilder, tableRecordInfo, dictionaryMap);
    }

    private Dictionary<?>[] buildDictionary(List<List<String>> table, IIDesc desc) {
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

        Dictionary<?>[] result = new Dictionary<?>[allColumns.size()];
        for (TblColRef tblColRef : valueMap.keySet()) {
            final Collection<byte[]> bytes = Collections2.transform(valueMap.get(tblColRef), new Function<String, byte[]>() {
                @Nullable
                @Override
                public byte[] apply(String input) {
                    return input == null ? null : input.getBytes();
                }
            });
            logger.info("build dictionary for column " + tblColRef);
            final Dictionary<?> dict = DictionaryGenerator.buildDictionaryFromValueList(tblColRef.getType(), bytes);
            result[desc.findColumn(tblColRef)] = dict;
        }
        return result;
    }

    private Slice build(List<List<String>> table, BatchSliceBuilder sliceBuilder, final TableRecordInfo tableRecordInfo, Dictionary<?>[] localDictionary) {
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
}
