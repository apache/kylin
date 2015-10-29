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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

/**
 * @author yangli9
 */
public class InvertedIndexMapper<KEYIN> extends KylinMapper<KEYIN, Object, LongWritable, ImmutableBytesWritable> {

    private TableRecordInfo info;
    private TableRecord rec;

    private LongWritable outputKey;
    private ImmutableBytesWritable outputValue;
    private IMRInput.IMRTableInputFormat flatTableInputFormat;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        IIManager mgr = IIManager.getInstance(config);
        IIInstance ii = mgr.getII(conf.get(BatchConstants.CFG_II_NAME));
        IISegment seg = ii.getSegment(conf.get(BatchConstants.CFG_II_SEGMENT_NAME), SegmentStatusEnum.NEW);
        this.info = new TableRecordInfo(seg);
        this.rec = this.info.createTableRecord();

        outputKey = new LongWritable();
        outputValue = new ImmutableBytesWritable(rec.getBytes());

        flatTableInputFormat = MRUtil.getBatchCubingInputSide(ii.getFirstSegment()).getFlatTableInputFormat();
    }

    @Override
    public void map(KEYIN key, Object record, Context context) throws IOException, InterruptedException {

        String[] row = flatTableInputFormat.parseMapperInput(record);
        rec.reset();
        for (int i = 0; i < row.length; i++) {
            Object fieldValue = row[i];
            if (fieldValue != null)
                rec.setValueString(i, fieldValue.toString());
        }

        outputKey.set(rec.getTimestamp());
        // outputValue's backing bytes array is the same as rec

        context.write(outputKey, outputValue);
    }
}
