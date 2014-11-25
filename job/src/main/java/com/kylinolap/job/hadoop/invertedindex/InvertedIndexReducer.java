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
import org.apache.hadoop.mapreduce.Reducer;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.invertedindex.IIKeyValueCodec;
import com.kylinolap.cube.invertedindex.TableRecord;
import com.kylinolap.cube.invertedindex.TableRecordInfo;
import com.kylinolap.cube.invertedindex.Slice;
import com.kylinolap.cube.invertedindex.SliceBuilder;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * @author yangli9
 */
public class InvertedIndexReducer extends Reducer<LongWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

    private TableRecordInfo info;
    private TableRecord rec;
    private SliceBuilder builder;
    private IIKeyValueCodec kv;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        CubeManager mgr = CubeManager.getInstance(config);
        CubeInstance cube = mgr.getCube(conf.get(BatchConstants.CFG_CUBE_NAME));
        CubeSegment seg = cube.getSegment(conf.get(BatchConstants.CFG_CUBE_SEGMENT_NAME), CubeSegmentStatusEnum.NEW);
        info = new TableRecordInfo(seg);
        rec = new TableRecord(info);
        builder = null;
        kv = new IIKeyValueCodec(info);
    }

    @Override
    public void reduce(LongWritable key, Iterable<ImmutableBytesWritable> values, Context context) //
            throws IOException, InterruptedException {
        for (ImmutableBytesWritable v : values) {
            rec.setBytes(v.get(), v.getOffset(), v.getLength());

            if (builder == null) {
                builder = new SliceBuilder(info, rec.getShard());
            }
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
