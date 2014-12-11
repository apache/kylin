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

package com.kylinolap.storage.hbase;


import java.io.IOException;
import java.util.*;

import com.kylinolap.invertedindex.IIInstance;
import com.kylinolap.invertedindex.IISegment;
import com.kylinolap.invertedindex.index.Slice;
import com.kylinolap.invertedindex.index.TableRecord;
import com.kylinolap.invertedindex.index.TableRecordBytes;
import com.kylinolap.invertedindex.index.TableRecordInfo;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.invertedindex.model.IIKeyValueCodec;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.storage.hbase.coprocessor.endpoint.EndpointTupleIterator;
import org.apache.hadoop.hbase.client.HConnection;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.HBaseConnection;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.storage.IStorageEngine;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.tuple.ITupleIterator;
import com.kylinolap.storage.tuple.Tuple;
import com.kylinolap.storage.tuple.TupleInfo;

/**
 * @author yangli9
 */
public class InvertedIndexStorageEngine implements IStorageEngine {

    private String hbaseUrl;
    private IISegment seg;
    private ColumnDesc[] columnDescs;

    public InvertedIndexStorageEngine(IIInstance ii) {
        this.seg = ii.getFirstSegment();
        IIDesc cubeDesc = this.seg.getIIDesc();
        this.columnDescs = MetadataManager.getInstance(cubeDesc.getConfig()).
                getTableDesc(cubeDesc.getFactTable()).getColumns();
        this.hbaseUrl = KylinConfig.getInstanceFromEnv().getStorageUrl();
    }

    @Override
    public ITupleIterator search(Collection<TblColRef> dimensions, TupleFilter filter, Collection<TblColRef> groups, Collection<FunctionDesc> metrics, StorageContext context) {
        String tableName = seg.getStorageLocationIdentifier();

        //HConnection is cached, so need not be closed
        HConnection conn = HBaseConnection.get(context.getConnUrl());
        try {
            return new EndpointTupleIterator(seg, columnDescs, filter, groups, new ArrayList(metrics), context, conn);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new IllegalStateException("Error when connecting to II htable " + tableName, e);
        }
    }

    private class IISegmentTupleIterator implements ITupleIterator {
        final StorageContext context;
        final HBaseClientKVIterator kvIterator;
        final IIKeyValueCodec codec;
        final Iterator<Slice> sliceIterator;
        Iterator<TableRecordBytes> recordIterator;
        Tuple next;

        TupleInfo tupleInfo;
        Tuple tuple;

        IISegmentTupleIterator(StorageContext context) throws IOException {
            this.context = context;

            HConnection hconn = HBaseConnection.get(hbaseUrl);
            String tableName = seg.getStorageLocationIdentifier();
            kvIterator = new HBaseClientKVIterator(hconn, tableName, IIDesc.HBASE_FAMILY_BYTES, IIDesc.HBASE_QUALIFIER_BYTES);
            codec = new IIKeyValueCodec(new TableRecordInfo(seg));
            sliceIterator = codec.decodeKeyValue(kvIterator).iterator();
        }

        private TupleInfo buildTupleInfo(TableRecordInfo recInfo) {
            TupleInfo info = new TupleInfo();
            ColumnDesc[] columns = recInfo.getColumns();
            for (int i = 0; i < columns.length; i++) {
                TblColRef col = new TblColRef(columns[i]);
                info.setField(context.getFieldName(col), col, col.getType().getName(), i);
            }
            return info;
        }

        private Tuple toTuple(TableRecord rec) {
            if (tuple == null) {
                tupleInfo = buildTupleInfo(rec.info());
                tuple = new Tuple(tupleInfo);
            }

            List<String> fieldNames = tupleInfo.getAllFields();
            for (int i = 0, n = tupleInfo.size(); i < n; i++) {
                tuple.setDimensionValue(fieldNames.get(i), rec.getValueString(i));
            }
            return tuple;
        }

        @Override
        public boolean hasNext() {
            while (next == null) {
                if (recordIterator != null && recordIterator.hasNext()) {
                    next = toTuple((TableRecord) recordIterator.next());
                    break;
                }
                if (sliceIterator.hasNext()) {
                    recordIterator = sliceIterator.next().iterator();
                    continue;
                }
                break;
            }

            return next != null;
        }

        @Override
        public Tuple next() {
            if (next == null)
                throw new NoSuchElementException();

            Tuple r = next;
            next = null;
            return r;
        }

        @Override
        public void close() {
            kvIterator.close();
        }

    }

}
