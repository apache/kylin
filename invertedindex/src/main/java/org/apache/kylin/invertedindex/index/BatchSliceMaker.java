/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.invertedindex.index;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.invertedindex.model.IIDesc;

import java.util.List;

/**
 * Created by qianzhou on 3/20/15.
 */
public class BatchSliceMaker {

    private final int nColumns;
    private final int nRecordsCap;
    private final short shard;
    private final IIDesc desc;

    private long sliceTimestamp;

    transient ImmutableBytesWritable temp = new ImmutableBytesWritable();

    public BatchSliceMaker(IIDesc desc, short shard) {
        this.desc = desc;
        this.nColumns = desc.listAllColumns().size();
        this.nRecordsCap = Math.max(1, desc.getSliceSize());

        this.shard = shard;
        this.sliceTimestamp = Long.MIN_VALUE;
    }

    public Slice makeSlice(TableRecordInfoDigest digest, List<TableRecord> records) {
        Preconditions.checkArgument(records != null && !records.isEmpty(), "records cannot be empty");
        Preconditions.checkArgument(records.size() <= nRecordsCap, "batch count cannot exceed " + nRecordsCap);
        sliceTimestamp = increaseSliceTimestamp(records.get(0).getTimestamp());
        ColumnValueContainer[] containers = new ColumnValueContainer[nColumns];
        for (int i : desc.getValueColumns()) {
            containers[i] = new CompressedValueContainer(digest, i, nRecordsCap);
        }
        for (int i : desc.getMetricsColumns()) {
            containers[i] = new CompressedValueContainer(digest, i, nRecordsCap);
        }
        for (TableRecord record : records) {
            for (int i = 0; i < nColumns; i++) {
                record.getValueBytes(i, temp);
                containers[i].append(temp);
            }
        }
        return new Slice(digest, shard, sliceTimestamp, containers);

    }

    private long increaseSliceTimestamp(long timestamp) {
        if (timestamp <= sliceTimestamp) {
            return sliceTimestamp + 1; // ensure slice timestamp increases
        } else {
            return timestamp;
        }
    }

}
