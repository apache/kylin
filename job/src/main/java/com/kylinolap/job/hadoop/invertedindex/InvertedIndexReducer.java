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
package com.kylinolap.job.hadoop.invertedindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.mr.KylinReducer;
import com.kylinolap.invertedindex.IIInstance;
import com.kylinolap.invertedindex.IIManager;
import com.kylinolap.invertedindex.IISegment;
import com.kylinolap.invertedindex.index.Slice;
import com.kylinolap.invertedindex.index.SliceBuilder;
import com.kylinolap.invertedindex.index.TableRecord;
import com.kylinolap.invertedindex.index.TableRecordInfo;
import com.kylinolap.invertedindex.model.IIKeyValueCodec;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.metadata.model.SegmentStatusEnum;

/**
 * @author yangli9
 */
public class InvertedIndexReducer extends KylinReducer<LongWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

    private TableRecordInfo info;
    private TableRecord rec;
    private SliceBuilder builder;
    private IIKeyValueCodec kv;

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        IIManager mgr = IIManager.getInstance(config);
        IIInstance ii = mgr.getII(conf.get(BatchConstants.CFG_II_NAME));
        IISegment seg = ii.getSegment(conf.get(BatchConstants.CFG_II_SEGMENT_NAME), SegmentStatusEnum.NEW);
        info = new TableRecordInfo(seg);
        rec = info.createTableRecord();
        builder = null;
        kv = new IIKeyValueCodec(info.getDigest());
    }

    @Override
    public void reduce(LongWritable key, Iterable<ImmutableBytesWritable> values, Context context) //
            throws IOException, InterruptedException {
        for (ImmutableBytesWritable v : values) {
            rec.setBytes(v.get(), v.getOffset(), v.getLength());

            if (builder == null) {
                builder = new SliceBuilder(info, rec.getShard());
            }

            //TODO: to delete this log
            System.out.println(rec.getShard() + " - " + rec);

            Slice slice = builder.append(rec);
            if (slice != null) {
                output(slice, context);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Slice slice = builder.close();
        if (slice != null) {
            output(slice, context);
        }
    }

    private void output(Slice slice, Context context) throws IOException, InterruptedException {
        for (Pair<ImmutableBytesWritable, ImmutableBytesWritable> pair : kv.encodeKeyValue(slice)) {
            context.write(pair.getFirst(), pair.getSecond());
        }
    }

}
