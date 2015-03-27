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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

/**
 * @author yangli9
 */
public class InvertedIndexMapper<KEYIN> extends KylinMapper<KEYIN, HCatRecord, LongWritable, ImmutableBytesWritable> {

    private TableRecordInfo info;
    private TableRecord rec;

    private LongWritable outputKey;
    private ImmutableBytesWritable outputValue;
    private HCatSchema schema = null;
    private List<HCatFieldSchema> fields;
    
    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        IIManager mgr = IIManager.getInstance(config);
        IIInstance ii = mgr.getII(conf.get(BatchConstants.CFG_II_NAME));
        IISegment seg = ii.getSegment(conf.get(BatchConstants.CFG_II_SEGMENT_NAME), SegmentStatusEnum.NEW);
        this.info = new TableRecordInfo(seg);
        this.rec = this.info.createTableRecord();

        outputKey = new LongWritable();
        outputValue = new ImmutableBytesWritable(rec.getBytes());

        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        
        fields = schema.getFields();
    }

    @Override
    public void map(KEYIN key, HCatRecord record, Context context) throws IOException, InterruptedException {

        rec.reset();
        for (int i = 0; i < fields.size(); i++) {
            Object fieldValue = record.get(i);
            rec.setValueString(i, fieldValue == null? null : fieldValue.toString());
        }

        outputKey.set(rec.getTimestamp());
        // outputValue's backing bytes array is the same as rec

        context.write(outputKey, outputValue);
    }
}
