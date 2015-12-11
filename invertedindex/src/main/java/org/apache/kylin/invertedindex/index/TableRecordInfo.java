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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.measure.fixedlen.FixedLenMeasureCodec;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author yangli9
 *         <p/>
 *         TableRecordInfo stores application-aware knowledges, while
 *         TableRecordInfoDigest only stores byte level knowleges
 */
public class TableRecordInfo {

    final IISegment seg;
    final IIDesc desc;
    final int nColumns;
    final List<TblColRef> allColumns;

    final FixedLenMeasureCodec<?>[] measureSerializers;
    final Dictionary<?>[] dictionaries;

    final TableRecordInfoDigest digest;

    public TableRecordInfo(IISegment iiSegment) {

        seg = iiSegment;
        desc = seg.getIIInstance().getDescriptor();
        allColumns = desc.listAllColumns();
        nColumns = allColumns.size();
        dictionaries = new Dictionary<?>[nColumns];
        measureSerializers = new FixedLenMeasureCodec<?>[nColumns];

        DictionaryManager dictMgr = DictionaryManager.getInstance(desc.getConfig());
        int index = 0;
        for (TblColRef tblColRef : desc.listAllColumns()) {
            ColumnDesc col = tblColRef.getColumn();
            if (desc.isMetricsCol(index)) {
                measureSerializers[index] = FixedLenMeasureCodec.get(col.getType());
            } else {
                String dictPath = seg.getDictResPath(tblColRef);
                try {
                    dictionaries[index] = dictMgr.getDictionary(dictPath);
                } catch (IOException e) {
                    throw new RuntimeException("dictionary " + dictPath + " does not exist ", e);
                }
            }
            index++;
        }

        digest = createDigest();
    }

    public TableRecordInfoDigest getDigest() {
        return digest;
    }

    private TableRecordInfoDigest createDigest() {
        // isMetric
        boolean[] isMetric = new boolean[nColumns];
        for (int i = 0; i < nColumns; ++i) {
            isMetric[i] = desc.isMetricsCol(i);
        }

        // lengths
        int[] lengths = new int[nColumns];
        for (int i = 0; i < nColumns; ++i) {
            lengths[i] = isMetric[i] ? measureSerializers[i].getLength() : dictionaries[i].getSizeOfId();
        }

        // dict max id
        int[] dictMaxIds = new int[nColumns];
        for (int i = 0; i < nColumns; ++i) {
            if (!isMetric[i])
                dictMaxIds[i] = dictionaries[i].getMaxId();
        }

        // offsets
        int pos = 0;
        int[] offsets = new int[nColumns];
        for (int i = 0; i < nColumns; i++) {
            offsets[i] = pos;
            pos += lengths[i];
        }

        int byteFormLen = pos;

        return new TableRecordInfoDigest(nColumns, byteFormLen, offsets, dictMaxIds, lengths, isMetric, measureSerializers);
    }

    public TableRecord createTableRecord() {
        return new TableRecord(digest.createTableRecordBytes(), this);
    }

    public IIDesc getDescriptor() {
        return desc;
    }

    public List<TblColRef> getColumns() {
        return allColumns;
    }

    public int findColumn(TblColRef col) {
        return desc.findColumn(col);
    }

    public int findFactTableColumn(String columnName) {
        if (columnName == null)
            return -1;
        for (int i = 0; i < allColumns.size(); ++i) {
            TblColRef tblColRef = allColumns.get(i);
            if (tblColRef.isSameAs(desc.getFactTableName(), columnName)) {
                return i;
            }
        }
        return -1;
    }

    // dimensions go with dictionary
    @SuppressWarnings("unchecked")
    public Dictionary<String> dict(int col) {
        // yes, all dictionaries are string based
        return (Dictionary<String>) dictionaries[col];
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
