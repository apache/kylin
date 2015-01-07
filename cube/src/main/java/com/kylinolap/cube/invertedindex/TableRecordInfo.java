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

package com.kylinolap.cube.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.measure.fixedlen.FixedLenMeasureCodec;
import com.kylinolap.dict.Dictionary;
import com.kylinolap.dict.DictionaryManager;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;

/**
 * @author yangli9
 *         <p/>
 *         TableRecordInfo stores application-aware knowledges,
 *         while TableRecordInfoDigest only stores byte level knowleges
 */
public class TableRecordInfo extends TableRecordInfoDigest {

    final CubeSegment seg;
    final InvertedIndexDesc desc;
    final TableDesc tableDesc;

    final String[] colNames;
    final Dictionary<?>[] dictionaries;
    final FixedLenMeasureCodec<?>[] measureSerializers;


    public TableRecordInfo(CubeSegment cubeSeg) throws IOException {

        seg = cubeSeg;
        desc = seg.getCubeInstance().getInvertedIndexDesc();
        tableDesc = desc.getFactTableDesc();

        nColumns = tableDesc.getColumnCount();
        colNames = new String[nColumns];
        dictionaries = new Dictionary<?>[nColumns];
        measureSerializers = new FixedLenMeasureCodec<?>[nColumns];

        DictionaryManager dictMgr = DictionaryManager.getInstance(desc.getConfig());
        for (ColumnDesc col : tableDesc.getColumns()) {
            int i = col.getZeroBasedIndex();
            colNames[i] = col.getName();
            if (desc.isMetricsCol(i)) {
                measureSerializers[i] = FixedLenMeasureCodec.get(col.getType());
            } else {
                String dictPath = seg.getDictResPath(new TblColRef(col));
                dictionaries[i] = dictMgr.getDictionary(dictPath);
            }
        }

        //isMetric
        isMetric = new boolean[nColumns];
        for (int i = 0; i < nColumns; ++i) {
            isMetric[i] = desc.isMetricsCol(i);
        }

        //lengths
        lengths = new int[nColumns];
        for (int i = 0; i < nColumns; ++i) {
            lengths[i] = isMetrics(i) ? measureSerializers[i].getLength() : dictionaries[i].getSizeOfId();
        }

        //dict max id
        dictMaxIds = new int[nColumns];
        for (int i = 0; i < nColumns; ++i) {
            if (!isMetrics(i))
                dictMaxIds[i] = dictionaries[i].getMaxId();
        }

        //offsets
        int pos = 0;
        offsets = new int[nColumns];
        for (int i = 0; i < nColumns; i++) {
            offsets[i] = pos;
            pos += length(i);
        }

        byteFormLen = pos;
    }

    @Override
    public TableRecordBytes createTableRecord() {
        return new TableRecord(this);
    }

    public InvertedIndexDesc getDescriptor() {
        return desc;
    }

    public ColumnDesc[] getColumns() {
        return tableDesc.getColumns();
    }


    // dimensions go with dictionary
    @SuppressWarnings("unchecked")
    public Dictionary<String> dict(int col) {
        // yes, all dictionaries are string based
        return (Dictionary<String>) dictionaries[col];
    }

    // metrics go with fixed-len codec
    @SuppressWarnings("unchecked")
    public FixedLenMeasureCodec<LongWritable> codec(int col) {
        // yes, all metrics are long currently
        return (FixedLenMeasureCodec<LongWritable>) measureSerializers[col];
    }


    public int getTimestampColumn() {
        return desc.getTimestampColumn();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((seg == null) ? 0 : seg.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableRecordInfo other = (TableRecordInfo) obj;
        if (seg == null) {
            if (other.seg != null)
                return false;
        } else if (!seg.equals(other.seg))
            return false;
        return true;
    }

}
