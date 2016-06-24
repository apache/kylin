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

package org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.storage.hbase.steps.RowValueDecoder;

/**
 * @author yangli9
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ObserverAggregators {

    public static ObserverAggregators fromValueDecoders(Collection<RowValueDecoder> rowValueDecoders) {

        // each decoder represents one HBase column
        HCol[] hcols = new HCol[rowValueDecoders.size()];
        int i = 0;
        for (RowValueDecoder rowValueDecoder : rowValueDecoders) {
            hcols[i++] = buildHCol(rowValueDecoder.getHBaseColumn());
        }

        ObserverAggregators aggrs = new ObserverAggregators(hcols);
        return aggrs;

    }

    private static HCol buildHCol(HBaseColumnDesc desc) {
        byte[] family = Bytes.toBytes(desc.getColumnFamilyName());
        byte[] qualifier = Bytes.toBytes(desc.getQualifier());
        MeasureDesc[] measures = desc.getMeasures();

        String[] funcNames = new String[measures.length];
        String[] dataTypes = new String[measures.length];

        for (int i = 0; i < measures.length; i++) {
            funcNames[i] = measures[i].getFunction().getExpression();
            dataTypes[i] = measures[i].getFunction().getReturnType();
        }

        return new HCol(family, qualifier, funcNames, dataTypes);
    }

    public static byte[] serialize(ObserverAggregators o) {
        ByteBuffer buf = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }

    public static ObserverAggregators deserialize(byte[] bytes) {
        return serializer.deserialize(ByteBuffer.wrap(bytes));
    }

    private static final BytesSerializer<ObserverAggregators> serializer = new BytesSerializer<ObserverAggregators>() {

        @Override
        public void serialize(ObserverAggregators value, ByteBuffer out) {
            BytesUtil.writeVInt(value.nHCols, out);
            for (int i = 0; i < value.nHCols; i++) {
                HCol col = value.hcols[i];
                BytesUtil.writeByteArray(col.family, out);
                BytesUtil.writeByteArray(col.qualifier, out);
                BytesUtil.writeAsciiStringArray(col.funcNames, out);
                BytesUtil.writeAsciiStringArray(col.dataTypes, out);
            }
        }

        @Override
        public ObserverAggregators deserialize(ByteBuffer in) {
            int nHCols = BytesUtil.readVInt(in);
            HCol[] hcols = new HCol[nHCols];
            for (int i = 0; i < nHCols; i++) {
                byte[] family = BytesUtil.readByteArray(in);
                byte[] qualifier = BytesUtil.readByteArray(in);
                String[] funcNames = BytesUtil.readAsciiStringArray(in);
                String[] dataTypes = BytesUtil.readAsciiStringArray(in);
                hcols[i] = new HCol(family, qualifier, funcNames, dataTypes);
            }
            return new ObserverAggregators(hcols);
        }

    };

    // ============================================================================

    final HCol[] hcols;
    final int nHCols;
    final ByteBuffer[] hColValues;
    final int nTotalMeasures;

    MeasureType[] measureTypes;

    public ObserverAggregators(HCol[] _hcols) {
        this.hcols = sort(_hcols);
        this.nHCols = hcols.length;
        this.hColValues = new ByteBuffer[nHCols];

        int nTotalMeasures = 0;
        for (HCol col : hcols)
            nTotalMeasures += col.nMeasures;
        this.nTotalMeasures = nTotalMeasures;
    }

    private HCol[] sort(HCol[] hcols) {
        HCol[] copy = Arrays.copyOf(hcols, hcols.length);
        Arrays.sort(copy, new Comparator<HCol>() {
            @Override
            public int compare(HCol o1, HCol o2) {
                int comp = Bytes.compareTo(o1.family, o2.family);
                if (comp != 0)
                    return comp;
                comp = Bytes.compareTo(o1.qualifier, o2.qualifier);
                return comp;
            }
        });
        return copy;
    }

    public MeasureAggregator[] createBuffer() {
        if (measureTypes == null) {
            measureTypes = new MeasureType[nTotalMeasures];
            int i = 0;
            for (HCol col : hcols) {
                for (int j = 0; j < col.nMeasures; j++)
                    measureTypes[i++] = MeasureTypeFactory.create(col.funcNames[j], DataType.getType(col.dataTypes[j]));
            }
        }

        MeasureAggregator[] aggrs = new MeasureAggregator[nTotalMeasures];
        for (int i = 0; i < nTotalMeasures; i++) {
            aggrs[i] = measureTypes[i].newAggregator();
        }
        return aggrs;
    }

    public void aggregate(MeasureAggregator[] measureAggrs, List<Cell> rowCells) {
        int i = 0;
        for (int ci = 0; ci < nHCols; ci++) {
            HCol col = hcols[ci];
            Cell cell = findCell(col, rowCells);

            if (cell == null) {
                i += col.nMeasures;
                continue;
            }

            ByteBuffer input = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            col.measureCodec.decode(input, col.measureValues);
            for (int j = 0; j < col.nMeasures; j++)
                measureAggrs[i++].aggregate(col.measureValues[j]);
        }
    }

    private Cell findCell(HCol col, List<Cell> cells) {
        // cells are ordered by timestamp asc, thus search from back, first hit
        // is the latest version
        for (int i = cells.size() - 1; i >= 0; i--) {
            Cell cell = cells.get(i);
            if (match(col, cell)) {
                return cell;
            }
        }
        return null;
    }

    public static boolean match(HCol col, Cell cell) {
        return Bytes.compareTo(col.family, 0, col.family.length, cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) == 0 && Bytes.compareTo(col.qualifier, 0, col.qualifier.length, cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) == 0;
    }

    public int getHColsNum() {
        return nHCols;
    }

    public byte[][] getHColFamilies() {
        byte[][] result = new byte[nHCols][];
        for (int i = 0; i < nHCols; i++)
            result[i] = hcols[i].family;
        return result;
    }

    public byte[][] getHColQualifiers() {
        byte[][] result = new byte[nHCols][];
        for (int i = 0; i < nHCols; i++)
            result[i] = hcols[i].qualifier;
        return result;
    }

    public ByteBuffer[] getHColValues(MeasureAggregator[] aggrs) {
        int i = 0;
        for (int ci = 0; ci < nHCols; ci++) {
            HCol col = hcols[ci];
            for (int j = 0; j < col.nMeasures; j++)
                col.measureValues[j] = aggrs[i++].getState();

            hColValues[ci] = col.measureCodec.encode(col.measureValues);
        }
        return hColValues;
    }

    // ============================================================================

    public static class HCol {
        final byte[] family;
        final byte[] qualifier;
        final String[] funcNames;
        final String[] dataTypes;
        final int nMeasures;

        final BufferedMeasureEncoder measureCodec;
        final Object[] measureValues;

        public HCol(byte[] bFamily, byte[] bQualifier, String[] funcNames, String[] dataTypes) {
            this.family = bFamily;
            this.qualifier = bQualifier;
            this.funcNames = funcNames;
            this.dataTypes = dataTypes;
            this.nMeasures = funcNames.length;
            assert funcNames.length == dataTypes.length;

            this.measureCodec = new BufferedMeasureEncoder(dataTypes);
            this.measureValues = new Object[nMeasures];
        }

        @Override
        public String toString() {
            return "HCol [bFamily=" + Bytes.toString(family) + ", bQualifier=" + Bytes.toString(qualifier) + ", nMeasures=" + nMeasures + "]";
        }
    }

}
