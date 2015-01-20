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

import com.kylinolap.invertedindex.IIInstance;
import com.kylinolap.invertedindex.IIManager;
import com.kylinolap.invertedindex.IISegment;
import com.kylinolap.invertedindex.index.TableRecord;
import com.kylinolap.invertedindex.index.TableRecordInfo;
import com.kylinolap.metadata.model.SegmentStatusEnum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.mr.KylinMapper;
import com.kylinolap.common.util.BytesSplitter;
import com.kylinolap.common.util.SplittedBytes;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * @author yangli9
 */
public class InvertedIndexMapper<KEYIN> extends KylinMapper<KEYIN, Text, LongWritable, ImmutableBytesWritable> {

    private TableRecordInfo info;
    private TableRecord rec;
    private int delim;
    private BytesSplitter splitter;

    private LongWritable outputKey;
    private ImmutableBytesWritable outputValue;

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        String inputDelim = conf.get(BatchConstants.INPUT_DELIM);
        this.delim = inputDelim == null ? -1 : inputDelim.codePointAt(0);
        this.splitter = new BytesSplitter(200, 4096);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        IIManager mgr = IIManager.getInstance(config);
        IIInstance ii = mgr.getII(conf.get(BatchConstants.CFG_II_NAME));
        IISegment seg = ii.getSegment(conf.get(BatchConstants.CFG_II_SEGMENT_NAME), SegmentStatusEnum.NEW);
        this.info = new TableRecordInfo(seg);
        this.rec = this.info.createTableRecord();

        outputKey = new LongWritable();
        outputValue = new ImmutableBytesWritable(rec.getBytes());
    }

    @Override
    public void map(KEYIN key, Text value, Context context) throws IOException, InterruptedException {
        if (delim == -1) {
            delim = splitter.detectDelim(value, info.getDigest().getColumnCount());
        }

        int nParts = splitter.split(value.getBytes(), value.getLength(), (byte) delim);
        SplittedBytes[] parts = splitter.getSplitBuffers();

        if (nParts != info.getDigest().getColumnCount()) {
            throw new RuntimeException("Got " + parts.length + " from -- " + value.toString() + " -- but only " + info.getDigest().getColumnCount() + " expected");
        }

        rec.reset();
        for (int i = 0; i < nParts; i++) {
            rec.setValueString(i, Bytes.toString(parts[i].value, 0, parts[i].length));
        }

        outputKey.set(rec.getTimestamp());
        // outputValue's backing bytes array is the same as rec

        context.write(outputKey, outputValue);
    }
}