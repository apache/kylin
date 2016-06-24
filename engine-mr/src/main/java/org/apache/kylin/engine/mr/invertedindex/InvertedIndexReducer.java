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

package org.apache.kylin.engine.mr.invertedindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StreamingBatch;
import org.apache.kylin.common.util.StreamingMessage;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.SliceBuilder;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.invertedindex.model.IIRow;

import com.google.common.collect.Lists;

/**
 */
public class InvertedIndexReducer extends KylinReducer<LongWritable, Object, ImmutableBytesWritable, ImmutableBytesWritable> {

    private TableRecordInfo info;
    private IIKeyValueCodec kv;
    private IMRInput.IMRTableInputFormat flatTableInputFormat;
    private SliceBuilder sliceBuilder;
    private ArrayList<StreamingMessage> messages;
    private int sliceSize;
    private ImmutableBytesWritable immutableBytesWritable;
    private ByteBuffer valueBuf;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        IIManager mgr = IIManager.getInstance(config);
        IIInstance ii = mgr.getII(conf.get(BatchConstants.CFG_II_NAME));
        IISegment seg = ii.getFirstSegment();
        info = new TableRecordInfo(seg);
        kv = new IIKeyValueCodec(info.getDigest());
        flatTableInputFormat = MRUtil.getBatchCubingInputSide(ii.getFirstSegment()).getFlatTableInputFormat();
        sliceSize = ii.getDescriptor().getSliceSize();
        short shard = (short) context.getTaskAttemptID().getTaskID().getId();
        System.out.println("Generating to shard - " + shard);
        sliceBuilder = new SliceBuilder(seg.getIIDesc(), shard);
        messages = Lists.newArrayListWithCapacity(sliceSize);
        immutableBytesWritable = new ImmutableBytesWritable();
        valueBuf = ByteBuffer.allocate(1024 * 1024); // 1MB
    }

    @Override
    public void reduce(LongWritable key, Iterable<Object> values, Context context) //
            throws IOException, InterruptedException {
        for (Object v : values) {
            String[] row = flatTableInputFormat.parseMapperInput(v);
            messages.add((parse(row)));
            if (messages.size() >= sliceSize) {
                buildAndOutput(new StreamingBatch(messages, Pair.newPair(System.currentTimeMillis(), System.currentTimeMillis())), context);
                messages = Lists.newArrayList();
            }
        }
    }

    private StreamingMessage parse(String[] row) {
        return new StreamingMessage(Lists.newArrayList(row), System.currentTimeMillis(), System.currentTimeMillis(), Collections.<String, Object> emptyMap());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!messages.isEmpty()) {
            buildAndOutput(new StreamingBatch(messages, Pair.newPair(System.currentTimeMillis(), System.currentTimeMillis())), context);
            messages.clear();
        }

    }

    private void buildAndOutput(StreamingBatch streamingBatch, Context context) throws IOException, InterruptedException {
        final Slice slice = sliceBuilder.buildSlice(streamingBatch);
        ImmutableBytesWritable value, dictionary;
        for (IIRow pair : kv.encodeKeyValue(slice)) {
            value = pair.getValue();
            dictionary = pair.getDictionary();
            int newLength = 4 + value.getLength() + dictionary.getLength();
            if (newLength > valueBuf.limit()) {
                valueBuf = ByteBuffer.allocate(newLength);
            }
            valueBuf.clear();
            valueBuf.putInt(value.getLength());
            valueBuf.put(value.get(), value.getOffset(), value.getLength());
            valueBuf.put(dictionary.get(), dictionary.getOffset(), dictionary.getLength());
            immutableBytesWritable.set(valueBuf.array(), 0, newLength);
            context.write(pair.getKey(), immutableBytesWritable);
        }
    }

}
