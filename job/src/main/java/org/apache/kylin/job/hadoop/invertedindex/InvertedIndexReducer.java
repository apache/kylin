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

package org.apache.kylin.job.hadoop.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinReducer;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.IncrementalSliceMaker;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.invertedindex.model.IIRow;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import java.io.IOException;

/**
 * @author yangli9
 */
public class InvertedIndexReducer extends KylinReducer<LongWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

    private TableRecordInfo info;
    private TableRecord rec;
    private IncrementalSliceMaker builder;
    private IIKeyValueCodec kv;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
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
                builder = new IncrementalSliceMaker(info, rec.getShard());
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
        for (IIRow pair : kv.encodeKeyValue(slice)) {
            context.write(pair.getKey(), pair.getValue());
        }
    }

}
