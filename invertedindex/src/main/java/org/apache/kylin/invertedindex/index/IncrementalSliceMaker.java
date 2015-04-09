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

package org.apache.kylin.invertedindex.index;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * @author yangli9
 */
public class IncrementalSliceMaker {

	TableRecordInfo info;
	private int nColumns;
	int nRecordsCap;

	short shard;
	long sliceTimestamp;
	int nRecords;
	private ColumnValueContainer[] containers;

	transient ImmutableBytesWritable temp = new ImmutableBytesWritable();

	public IncrementalSliceMaker(TableRecordInfo info, short shard) {
		this.info = info;
		this.nColumns = info.getDigest().getColumnCount();
		this.nRecordsCap = Math.max(1, info.getDescriptor().getSliceSize());

		this.shard = shard;
		this.sliceTimestamp = Long.MIN_VALUE;
		this.nRecords = 0;
		this.containers = null;

		doneSlice(); // init containers
	}

	private Slice doneSlice() {
		Slice r = null;
		if (nRecords > 0) {
			for (int i = 0; i < nColumns; i++) {
				containers[i].closeForChange();
			}
			r = new Slice(info.getDigest(), shard, sliceTimestamp, containers);
		}

		// reset for next slice
		nRecords = 0;
		containers = new ColumnValueContainer[nColumns];
//        for (int i : info.getDescriptor().getBitmapColumns()) {
//            containers[i] = new CompressedValueContainer(info.getDigest(), i,
//                    nRecordsCap);
//        }
		for (int i : info.getDescriptor().getValueColumns()) {
			containers[i] = new CompressedValueContainer(info.getDigest(), i,
					nRecordsCap);
		}
		for (int i : info.getDescriptor().getMetricsColumns()) {
			containers[i] = new CompressedValueContainer(info.getDigest(), i,
					nRecordsCap);
		}

		return r;

	}

	// NOTE: record must be appended in time order
	public Slice append(TableRecord rec) {
		if (rec.getShard() != shard)
			throw new IllegalStateException();

		Slice doneSlice = null;

		if (isFull()) {
			doneSlice = doneSlice();
		}

		if (nRecords == 0) {
			sliceTimestamp = increaseSliceTimestamp(rec.getTimestamp());
		}

		nRecords++;
		for (int i = 0; i < nColumns; i++) {
			rec.getValueBytes(i, temp);
			containers[i].append(temp);
		}

		return doneSlice;
	}

	private long increaseSliceTimestamp(long timestamp) {
		if (timestamp < sliceTimestamp)
			throw new IllegalStateException();

		if (timestamp == sliceTimestamp)
			return ++timestamp; // ensure slice timestamp increases
		else
			return timestamp;
	}

	public Slice close() {
		Slice doneSlice = doneSlice();
		this.sliceTimestamp = Long.MIN_VALUE;
		this.nRecords = 0;
		return doneSlice;
	}

	private boolean isFull() {
		return nRecords >= nRecordsCap;
	}
}
