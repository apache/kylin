/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.invertedindex.streaming;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.engine.streaming.StreamingBatch;
import org.apache.kylin.engine.streaming.StreamingMessage;
import org.apache.kylin.invertedindex.index.BatchSliceMaker;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.util.IIDictionaryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public final class SliceBuilder {

    private static Logger logger = LoggerFactory.getLogger(SliceBuilder.class);

    private final BatchSliceMaker sliceMaker;
    private final IIDesc iiDesc;
    private final boolean useLocalDict;

    public SliceBuilder(IIDesc desc, short shard, boolean useLocalDict) {
        this.iiDesc = desc;
        this.sliceMaker = new BatchSliceMaker(desc, shard);
        this.useLocalDict = useLocalDict;
    }

    public Slice buildSlice(StreamingBatch microStreamBatch) {
        final List<List<String>> messages = Lists.transform(microStreamBatch.getMessages(), new Function<StreamingMessage, List<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable StreamingMessage input) {
                return input.getData();
            }
        });
        final Dictionary<?>[] dictionaries = useLocalDict ? IIDictionaryBuilder.buildDictionary(messages, iiDesc) : new Dictionary[iiDesc.listAllColumns().size()];
        TableRecordInfo tableRecordInfo = new TableRecordInfo(iiDesc, dictionaries);
        return build(messages, tableRecordInfo, dictionaries);
    }

    private Slice build(List<List<String>> table, final TableRecordInfo tableRecordInfo, Dictionary<?>[] localDictionary) {
        final Slice slice = sliceMaker.makeSlice(tableRecordInfo.getDigest(), Lists.transform(table, new Function<List<String>, TableRecord>() {
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
