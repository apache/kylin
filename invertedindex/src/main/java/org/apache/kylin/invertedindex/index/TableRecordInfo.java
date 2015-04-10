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

import java.util.List;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.measure.fixedlen.FixedLenMeasureCodec;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author yangli9
 *         <p/>
 *         TableRecordInfo stores application-aware knowledges, while
 *         TableRecordInfoDigest only stores byte level knowleges
 */
public class TableRecordInfo {

    final IIDesc desc;

    final TableRecordInfoDigest digest;
    final Dictionary<?>[] dictionaries;

    public TableRecordInfo(IISegment iiSegment) {
        this(iiSegment.getIIDesc());
    }

    public TableRecordInfo(IIDesc desc) {
        this(desc, new Dictionary<?>[0]);
    }

    public TableRecordInfo(IIDesc desc, Dictionary<?>[] dictionaries) {
        this.desc = desc;
        this.dictionaries = dictionaries;
        this.digest = createDigest(desc, dictionaries);
    }

    public TableRecordInfoDigest getDigest() {
        return digest;
    }

    private TableRecordInfoDigest createDigest(IIDesc desc, Dictionary<?>[] dictionaryMap) {
        final List<TblColRef> tblColRefs = desc.listAllColumns();
        final int nColumns = tblColRefs.size();
        boolean[] isMetric = new boolean[nColumns];
        int[] lengths = new int[nColumns];
        int[] dictMaxIds = new int[nColumns];
        String[] dataTypes = new String[nColumns];
        for (int i = 0; i < nColumns; ++i) {
            final TblColRef tblColRef = tblColRefs.get(i);
            isMetric[i] = desc.isMetricsCol(i);
            dataTypes[i] = tblColRef.getDatatype();
            if (isMetric[i]) {
                lengths[i] = FixedLenMeasureCodec.get(DataType.getInstance(tblColRef.getColumn().getDatatype())).getLength();
            } else {
                if (Array.isEmpty(dictionaryMap)) {
                    final DataType dataType = DataType.getInstance(tblColRef.getColumn().getDatatype());
                    if (dataType.isNumberFamily()) {
                        lengths[i] = 16;
                    } else if (dataType.isStringFamily()) {
                        lengths[i] = 256;
                    } else if (dataType.isDateTimeFamily()) {
                        lengths[i] = 10;
                    } else {
                        throw new RuntimeException("invalid data type:" + dataType);
                    }
                    dictMaxIds[i] = Integer.MAX_VALUE;
                } else {
                    final Dictionary<?> dictionary = dictionaryMap[i];
                    lengths[i] = dictionary.getSizeOfId();
                    dictMaxIds[i] = dictionary.getMaxId();
                }
            }
        }
        // offsets
        int pos = 0;
        int[] offsets = new int[nColumns];
        for (int i = 0; i < nColumns; i++) {
            offsets[i] = pos;
            pos += lengths[i];
        }

        int byteFormLen = pos;

        return new TableRecordInfoDigest(nColumns, byteFormLen,offsets, dictMaxIds, lengths, isMetric, dataTypes);
    }

    public TableRecord createTableRecord() {
        return new TableRecord(digest.createTableRecordBytes(), this);
    }

    public final IIDesc getDescriptor() {
        return desc;
    }

    public final List<TblColRef> getColumns() {
        return desc.listAllColumns();
    }

    public int findColumn(TblColRef col) {
        return desc.findColumn(col);
    }

    public int findFactTableColumn(String columnName) {
        if (columnName == null)
            return -1;
        for (int i = 0; i < getColumns().size(); ++i) {
            TblColRef tblColRef = getColumns().get(i);
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

}
