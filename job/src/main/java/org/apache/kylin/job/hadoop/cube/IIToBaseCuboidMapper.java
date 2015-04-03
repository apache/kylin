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

package org.apache.kylin.job.hadoop.cube;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.FIFOIterable;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.RawTableRecord;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.invertedindex.model.*;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;

/**
 * honma
 */
public class IIToBaseCuboidMapper extends BaseCuboidMapperBase<ImmutableBytesWritable, Result> {
    private Queue<IIRow> buffer = Lists.newLinkedList();
    private Iterator<Slice> slices;

    @Override
    protected void setup(Context context) throws IOException {
        super.setup(context);

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String iiName = conf.get(BatchConstants.CFG_II_NAME);
        IIInstance ii = IIManager.getInstance(config).getII(iiName);
        IIDesc iiDesc = ii.getDescriptor();

        TableRecordInfo info = new TableRecordInfo(iiDesc);
        KeyValueCodec codec = new IIKeyValueCodecWithState(info.getDigest());
        slices = codec.decodeKeyValue(new FIFOIterable<IIRow>(buffer)).iterator();
    }

    @Override
    public void map(ImmutableBytesWritable key, Result cells, Context context) throws IOException, InterruptedException {
        try {
            IIRow iiRow = new IIRow();
            for (Cell c : cells.rawCells()) {
                iiRow.updateWith(c);
            }
            buffer.add(iiRow);

            if (slices.hasNext()) {
                Slice slice = slices.next();
                TableRecordInfoDigest localDigest = slice.getInfo();
                for (RawTableRecord record : slice) {

                    counter++;
                    if (counter % BatchConstants.COUNTER_MAX == 0) {
                        logger.info("Handled " + counter + " records!");
                    }

                    for (int indexInRecord = 0; indexInRecord < localDigest.getColumnCount(); ++indexInRecord) {
                        SplittedBytes columnBuffer = bytesSplitter.getSplitBuffer(indexInRecord);
                        if (!localDigest.isMetrics(indexInRecord)) {
                            String v = record.getValueMetric(indexInRecord);
                            byte[] metricBytes = v.getBytes();
                            System.arraycopy(metricBytes, 0, columnBuffer.value, 0, metricBytes.length);
                            columnBuffer.length = metricBytes.length;
                        } else {
                            Dictionary<?> dictionary = slice.getLocalDictionaries()[indexInRecord];
                            Preconditions.checkArgument(columnBuffer.value.length > dictionary.getSizeOfValue(), "Column length too big");
                            int vid = record.getValueID(indexInRecord);
                            columnBuffer.length = dictionary.getValueBytesFromId(vid, columnBuffer.value, 0);
                        }
                    }

                    outputKV(context);
                }
            }
        } catch (Exception ex) {
            handleErrorRecord(bytesSplitter, ex);
        }
    }
}
