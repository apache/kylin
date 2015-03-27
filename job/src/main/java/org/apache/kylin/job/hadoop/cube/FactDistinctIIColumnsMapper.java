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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.FIFOIterable;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.RawTableRecord;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.*;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.IntermediateColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * @author yangli9
 */
public class FactDistinctIIColumnsMapper extends FactDistinctColumnsMapperBase<ImmutableBytesWritable, Result> {

    private IIJoinedFlatTableDesc intermediateTableDesc;
    private Queue<IIRow> buffer = Lists.newLinkedList();
    private Iterator<Slice> slices;

    private String iiName;
    private IIInstance ii;
    private IIDesc iiDesc;

    private int[] baseCuboidCol2FlattenTableCol;

    @Override
    protected void setup(Context context) throws IOException {
        super.setup(context);

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        iiName = conf.get(BatchConstants.CFG_II_NAME);
        ii = IIManager.getInstance(config).getII(iiName);
        iiDesc = ii.getDescriptor();

        intermediateTableDesc = new IIJoinedFlatTableDesc(iiDesc);
        TableRecordInfo info = new TableRecordInfo(iiDesc);
        KeyValueCodec codec = new IIKeyValueCodecWithState(info.getDigest());
        slices = codec.decodeKeyValue(new FIFOIterable<IIRow>(buffer)).iterator();

        baseCuboidCol2FlattenTableCol = new int[factDictCols.size()];
        for (int i = 0; i < factDictCols.size(); ++i) {
            int index = findTblCol(intermediateTableDesc.getColumnList(), columns.get(factDictCols.get(i)));
            baseCuboidCol2FlattenTableCol[i] = index;
        }
    }

    private int findTblCol(List<IntermediateColumnDesc> columns, final TblColRef col) {
        return Iterators.indexOf(columns.iterator(), new Predicate<IntermediateColumnDesc>() {
            @Override
            public boolean apply(IntermediateColumnDesc input) {
                return input.getColRef().equals(col);
            }
        });
    }

    @Override
    public void map(ImmutableBytesWritable key, Result cells, Context context) throws IOException, InterruptedException {
        IIRow iiRow = new IIRow();
        for (Cell c : cells.rawCells()) {
            iiRow.updateWith(c);
        }
        buffer.add(iiRow);

        if (slices.hasNext()) {
            byte[] vBytesBuffer = null;
            Slice slice = slices.next();

            for (RawTableRecord record : slice) {
                for (int i = 0; i < factDictCols.size(); ++i) {
                    int baseCuboidIndex = factDictCols.get(i);
                    outputKey.set((short) baseCuboidIndex);
                    int indexInRecord = baseCuboidCol2FlattenTableCol[i];

                    Dictionary<?> dictionary = slice.getLocalDictionaries().get(indexInRecord);
                    if (vBytesBuffer == null || dictionary.getSizeOfValue() > vBytesBuffer.length) {
                        vBytesBuffer = new byte[dictionary.getSizeOfValue() * 2];
                    }

                    int vid = record.getValueID(baseCuboidIndex);
                    if (vid == dictionary.nullId()) {
                        continue;
                    }
                    int vBytesSize = dictionary.getValueBytesFromId(vid, vBytesBuffer, 0);

                    outputValue.set(vBytesBuffer, 0, vBytesSize);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

}
