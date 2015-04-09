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

package org.apache.kylin.storage.hbase.coprocessor;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Maps;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author yangli9
 */
public class CoprocessorRowType {

    //for endpoint
    public static CoprocessorRowType fromTableRecordInfo(TableRecordInfo tableRecordInfo, List<TblColRef> cols) {

        int[] colSizes = new int[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            colSizes[i] = tableRecordInfo.getDigest().length(i);
        }
        return new CoprocessorRowType(cols.toArray(new TblColRef[cols.size()]), colSizes);
    }

    //for observer
    public static CoprocessorRowType fromCuboid(CubeSegment seg, Cuboid cuboid) {
        List<TblColRef> colList = cuboid.getColumns();
        TblColRef[] cols = colList.toArray(new TblColRef[colList.size()]);
        RowKeyColumnIO colIO = new RowKeyColumnIO(seg);
        int[] colSizes = new int[cols.length];
        for (int i = 0; i < cols.length; i++) {
            colSizes[i] = colIO.getColumnLength(cols[i]);
        }
        return new CoprocessorRowType(cols, colSizes);
    }

    public static byte[] serialize(CoprocessorRowType o) {
        ByteBuffer buf = ByteBuffer.allocate(CoprocessorConstants.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }

    public static CoprocessorRowType deserialize(byte[] bytes) {
        return serializer.deserialize(ByteBuffer.wrap(bytes));
    }

    private static final Serializer serializer = new Serializer();

    private static class Serializer implements BytesSerializer<CoprocessorRowType> {

        @Override
        public void serialize(CoprocessorRowType o, ByteBuffer out) {
            int n = o.columns.length;
            BytesUtil.writeVInt(o.columns.length, out);
            for (int i = 0; i < n; i++) {
                BytesUtil.writeAsciiString(o.columns[i].getTable(), out);
                BytesUtil.writeAsciiString(o.columns[i].getName(), out);
                BytesUtil.writeVInt(o.columnSizes[i], out);
            }
        }

        @Override
        public CoprocessorRowType deserialize(ByteBuffer in) {
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
            return new CoprocessorRowType(cols, colSizes);
        }
    }

    // ============================================================================

    public TblColRef[] columns;
    public int[] columnSizes;
    public int[] columnOffsets;
    public HashMap<TblColRef, Integer> columnIdxMap;

    public CoprocessorRowType(TblColRef[] columns, int[] columnSizes) {
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
