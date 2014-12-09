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

package com.kylinolap.storage.hbase.coprocessor.observer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Maps;
import com.kylinolap.common.util.BytesSerializer;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.kv.RowConstants;
import com.kylinolap.cube.kv.RowKeyColumnIO;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.model.realization.TblColRef;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorConstants;

/**
 * @author yangli9
 */
public class ObserverRowType {

    public static ObserverRowType fromCuboid(CubeSegment seg, Cuboid cuboid) {
        List<TblColRef> colList = cuboid.getColumns();
        TblColRef[] cols = colList.toArray(new TblColRef[colList.size()]);
        RowKeyColumnIO colIO = new RowKeyColumnIO(seg);
        int[] colSizes = new int[cols.length];
        for (int i = 0; i < cols.length; i++) {
            colSizes[i] = colIO.getColumnLength(cols[i]);
        }
        return new ObserverRowType(cols, colSizes);
    }

    public static byte[] serialize(ObserverRowType o) {
        ByteBuffer buf = ByteBuffer.allocate(CoprocessorConstants.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }

    public static ObserverRowType deserialize(byte[] bytes) {
        return serializer.deserialize(ByteBuffer.wrap(bytes));
    }

    private static final Serializer serializer = new Serializer();

    private static class Serializer implements BytesSerializer<ObserverRowType> {

        @Override
        public void serialize(ObserverRowType o, ByteBuffer out) {
            int n = o.columns.length;
            BytesUtil.writeVInt(o.columns.length, out);
            for (int i = 0; i < n; i++) {
                BytesUtil.writeAsciiString(o.columns[i].getTable(), out);
                BytesUtil.writeAsciiString(o.columns[i].getName(), out);
                BytesUtil.writeVInt(o.columnSizes[i], out);
            }
        }

        @Override
        public ObserverRowType deserialize(ByteBuffer in) {
            int n = BytesUtil.readVInt(in);
            TblColRef[] cols = new TblColRef[n];
            int[] colSizes = new int[n];
            for (int i = 0; i < n; i++) {
                String tableName = BytesUtil.readAsciiString(in);
                String colName = BytesUtil.readAsciiString(in);
                TableDesc table = new TableDesc();
                table.setName(tableName);
                ColumnDesc col = new ColumnDesc();
                col.setTable(table);
                col.setName(colName);
                cols[i] = new TblColRef(col);

                int colSize = BytesUtil.readVInt(in);
                colSizes[i] = colSize;
            }
            return new ObserverRowType(cols, colSizes);
        }
    }

    // ============================================================================

    TblColRef[] columns;
    int[] columnSizes;

    int[] columnOffsets;
    List<TblColRef> columnsAsList;
    HashMap<TblColRef, Integer> columnIdxMap;

    public ObserverRowType(TblColRef[] columns, int[] columnSizes) {
        this.columns = columns;
        this.columnSizes = columnSizes;
        init();
    }

    public int getColIndexByTblColRef(TblColRef colRef) {
        return columnIdxMap.get(colRef);
    }

    private void init() {
        int[] offsets = new int[columns.length];
        int o = RowConstants.ROWKEY_CUBOIDID_LEN;
        for (int i = 0; i < columns.length; i++) {
            offsets[i] = o;
            o += columnSizes[i];
        }
        this.columnOffsets = offsets;

        this.columnsAsList = Arrays.asList(columns);

        HashMap<TblColRef, Integer> map = Maps.newHashMap();
        for (int i = 0; i < columns.length; i++) {
            map.put(columns[i], i);
        }
        this.columnIdxMap = map;
    }

    public int getColumnCount() {
        return columns.length;
    }

}
