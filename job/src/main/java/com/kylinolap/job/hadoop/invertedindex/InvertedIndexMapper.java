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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.common.BytesSplitter;
import com.kylinolap.cube.common.SplittedBytes;
import com.kylinolap.cube.invertedindex.TableRecord;
import com.kylinolap.cube.invertedindex.TableRecordInfo;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * @author yangli9
 * 
 */
public class InvertedIndexMapper<KEYIN> extends Mapper<KEYIN, Text, LongWritable, ImmutableBytesWritable> {

    private TableRecordInfo info;
    private TableRecord rec;
    private int delim;
    private BytesSplitter splitter;

    private LongWritable outputKey;
    private ImmutableBytesWritable outputValue;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String inputDelim = conf.get(BatchConstants.INPUT_DELIM);
        this.delim = inputDelim == null ? -1 : inputDelim.codePointAt(0);
        this.splitter = new BytesSplitter(200, 4096);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        CubeManager mgr = CubeManager.getInstance(config);
        CubeInstance cube = mgr.getCube(conf.get(BatchConstants.CFG_CUBE_NAME));
        CubeSegment seg = cube.getSegment(conf.get(BatchConstants.CFG_CUBE_SEGMENT_NAME), CubeSegmentStatusEnum.NEW);
        this.info = new TableRecordInfo(seg);
        this.rec = new TableRecord(this.info);

        outputKey = new LongWritable();
        outputValue = new ImmutableBytesWritable(rec.getBytes());
    }

    @Override
    public void map(KEYIN key, Text value, Context context) throws IOException, InterruptedException {
        if (delim == -1) {
            delim = splitter.detectDelim(value, info.getColumnCount());
        }

        int nParts = splitter.split(value.getBytes(), value.getLength(), (byte) delim);
        SplittedBytes[] parts = splitter.getSplitBuffers();

        if (nParts != info.getColumnCount()) {
            throw new RuntimeException("Got " + parts.length + " from -- " + value.toString() + " -- but only " + info.getColumnCount() + " expected");
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