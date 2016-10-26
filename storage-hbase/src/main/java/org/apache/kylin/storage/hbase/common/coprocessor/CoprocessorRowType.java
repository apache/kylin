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

package org.apache.kylin.storage.hbase.common.coprocessor;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;

/**
 * @author yangli9
 */
@Deprecated // used by v1 HBase coprocessor
public class CoprocessorRowType {

    //for observer
    public static CoprocessorRowType fromCuboid(CubeSegment seg, Cuboid cuboid) {
        List<TblColRef> colList = cuboid.getColumns();
        TblColRef[] cols = colList.toArray(new TblColRef[colList.size()]);
        RowKeyColumnIO colIO = new RowKeyColumnIO(seg.getDimensionEncodingMap());
        int[] colSizes = new int[cols.length];
        for (int i = 0; i < cols.length; i++) {
            colSizes[i] = colIO.getColumnLength(cols[i]);
        }
        return new CoprocessorRowType(cols, colSizes, seg.getRowKeyPreambleSize());
    }

    public static byte[] serialize(CoprocessorRowType o) {
        ByteBuffer buf = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }

    public static CoprocessorRowType deserialize(byte[] bytes) {
        return serializer.deserialize(ByteBuffer.wrap(bytes));
    }

    private static final BytesSerializer<CoprocessorRowType> serializer = new BytesSerializer<CoprocessorRowType>() {

        @Override
        public void serialize(CoprocessorRowType o, ByteBuffer out) {
            int n = o.columns.length;
            BytesUtil.writeVInt(o.columns.length, out);
            BytesUtil.writeVInt(o.bodyOffset, out);
            for (int i = 0; i < n; i++) {
                BytesUtil.writeAsciiString(o.columns[i].getTable(), out);
                BytesUtil.writeAsciiString(o.columns[i].getName(), out);
                BytesUtil.writeAsciiString(o.columns[i].getDatatype(), out);
                BytesUtil.writeVInt(o.columnSizes[i], out);
            }
        }

        @Override
        public CoprocessorRowType deserialize(ByteBuffer in) {
            int n = BytesUtil.readVInt(in);
            int bodyOffset = BytesUtil.readVInt(in);
            TblColRef[] cols = new TblColRef[n];
            int[] colSizes = new int[n];
            for (int i = 0; i < n; i++) {
                String tableName = BytesUtil.readAsciiString(in);
                String colName = BytesUtil.readAsciiString(in);
                String datatype = BytesUtil.readAsciiString(in);
                TableDesc table = new TableDesc();
                table.setName(tableName);
                ColumnDesc col = new ColumnDesc();
                col.setTable(table);
                col.setName(colName);
                col.setDatatype(datatype);
                col.init(table);
                cols[i] = col.getRef();

                int colSize = BytesUtil.readVInt(in);
                colSizes[i] = colSize;
            }
            return new CoprocessorRowType(cols, colSizes, bodyOffset);
        }
    };

    // ============================================================================

    public TblColRef[] columns;
    private int bodyOffset;
    public int[] columnSizes;
    public int[] columnOffsets;
    public HashMap<TblColRef, Integer> columnIdxMap;

    public CoprocessorRowType(TblColRef[] columns, int[] columnSizes, int bodyOffset) {
        this.bodyOffset = bodyOffset;
        this.columns = columns;
        this.columnSizes = columnSizes;
        init();
    }

    public int getColIndexByTblColRef(TblColRef colRef) {
        return columnIdxMap.get(colRef);
    }

    private void init() {
        int[] offsets = new int[columns.length];
        int o = bodyOffset;
        for (int i = 0; i < columns.length; i++) {
            offsets[i] = o;
            o += columnSizes[i];
        }
        this.columnOffsets = offsets;

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
