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

import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;

/**
 * @author yangli9
 */
public class InvertedIndexPartitioner extends Partitioner<LongWritable, ImmutableBytesWritable> implements Configurable {

    private Configuration conf;
    private TableRecordInfo info;
    private TableRecord rec;

    @Override
    public int getPartition(LongWritable key, ImmutableBytesWritable value, int numPartitions) {
        rec.setBytes(value.get(), value.getOffset(), value.getLength());
        return rec.getShard();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        try {
            KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
            IIManager mgr = IIManager.getInstance(config);
            IIInstance ii = mgr.getII(conf.get(BatchConstants.CFG_II_NAME));
            IISegment seg = ii.getSegment(conf.get(BatchConstants.CFG_II_SEGMENT_NAME), SegmentStatusEnum.NEW);
            this.info = new TableRecordInfo(seg);
            this.rec = this.info.createTableRecord();
        } catch (IOException e) {
            throw new RuntimeException("", e);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
