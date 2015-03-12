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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.kylin.dict.Dictionary;
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

    final IIDesc desc;

    final TableRecordInfoDigest digest;
    final Map<Integer, Dictionary<?>> dictionaryMap;

    public TableRecordInfo(IISegment iiSegment) {

        this.desc = iiSegment.getIIInstance().getDescriptor();
        this.dictionaryMap = Maps.newHashMap();
        Map<TblColRef, FixedLenMeasureCodec<?>> measureCodecMap = Maps.newHashMap();

        DictionaryManager dictMgr = DictionaryManager.getInstance(desc.getConfig());
        int index = 0;
        for (TblColRef tblColRef : desc.listAllColumns()) {
            ColumnDesc col = tblColRef.getColumn();
            if (desc.isMetricsCol(index)) {
                measureCodecMap.put(tblColRef, FixedLenMeasureCodec.get(col.getType()));
            } else {
                String dictPath = iiSegment.getDictResPath(tblColRef);
                if (dictPath != null) {
                    try {
                        dictionaryMap.put(index, dictMgr.getDictionary(dictPath));
                    } catch (IOException e) {
                        throw new RuntimeException("dictionary " + dictPath + " does not exist ", e);
                    }
                }
            }
            index++;
        }

        digest = createDigest(dictionaryMap, measureCodecMap);
    }

    public TableRecordInfo(IIDesc desc, Map<Integer, Dictionary<?>> dictionaryMap) {
        this.desc = desc;
        this.dictionaryMap = dictionaryMap;
        Map<TblColRef, FixedLenMeasureCodec<?>> measureCodecMap = Maps.newHashMap();
        int index = 0;
        for (TblColRef tblColRef : desc.listAllColumns()) {
            ColumnDesc col = tblColRef.getColumn();
            if (desc.isMetricsCol(index++)) {
                measureCodecMap.put(tblColRef, FixedLenMeasureCodec.get(col.getType()));
            }
        }
        this.digest = createDigest(dictionaryMap, measureCodecMap);
    }

    public TableRecordInfoDigest getDigest() {
        return digest;
    }

    private TableRecordInfoDigest createDigest(Map<Integer, Dictionary<?>> dictionaryMap, Map<TblColRef, FixedLenMeasureCodec<?>> measureCodecMap) {
        int nColumns = getColumns().size();
        boolean[] isMetric = new boolean[nColumns];
        int[] lengths = new int[nColumns];
        int[] dictMaxIds = new int[nColumns];
        String[] dataTypes = new String[nColumns];
        for (int i = 0; i < nColumns; ++i) {
            final TblColRef tblColRef = getColumns().get(i);
            isMetric[i] = desc.isMetricsCol(i);
            dataTypes[i] = tblColRef.getDatatype();
            if (isMetric[i]) {
                final FixedLenMeasureCodec<?> fixedLenMeasureCodec = measureCodecMap.get(tblColRef);
                if (fixedLenMeasureCodec != null) {
                    lengths[i] = fixedLenMeasureCodec.getLength();
                }
            } else {
                final Dictionary<?> dictionary = dictionaryMap.get(i);
                if (dictionary != null) {
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

        return new TableRecordInfoDigest(nColumns, byteFormLen, offsets, dictMaxIds, lengths, isMetric, dataTypes);
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
        return (Dictionary<String>) dictionaryMap.get(col);
    }

    public int getTimestampColumn() {
        return desc.getTimestampColumn();
    }

}
