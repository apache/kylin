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

import static com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.kylinolap.cube.invertedindex.*;
import org.apache.hadoop.hbase.client.HConnection;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.HBaseConnection;
import com.kylinolap.common.persistence.StorageException;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.metadata.model.cube.FunctionDesc;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.metadata.model.schema.ColumnDesc;
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
    private CubeSegment seg;

    public InvertedIndexStorageEngine(CubeInstance cube) {
        this.seg = cube.getFirstSegment();
        this.hbaseUrl = KylinConfig.getInstanceFromEnv().getStorageUrl();
    }

    @Override
    public ITupleIterator search(Collection<TblColRef> dimensions, TupleFilter filter, Collection<TblColRef> groups, Collection<FunctionDesc> metrics, StorageContext context) {

        try {
            return new IISegmentTupleIterator(context);
        } catch (IOException e) {
            throw new StorageException(e.getMessage(), e);
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
            kvIterator = new HBaseClientKVIterator(hconn, tableName, HBASE_FAMILY_BYTES, HBASE_QUALIFIER_BYTES);
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
