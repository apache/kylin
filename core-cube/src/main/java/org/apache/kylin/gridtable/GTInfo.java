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

package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.gridtable.CubeCodeSystem;
import org.apache.kylin.cube.gridtable.TrimmedCubeCodeSystem;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GTInfo {
    private static final Logger logger = LoggerFactory.getLogger(GTInfo.class);

    public static Builder builder() {
        return new Builder();
    }

    String tableName;
    IGTCodeSystem codeSystem;

    // column schema
    DataType[] colTypes;
    ImmutableBitSet colPreferIndex;
    int nColumns;
    ImmutableBitSet colAll;
    TblColRef[] colRefs;

    // grid info
    ImmutableBitSet primaryKey; // order by, uniqueness is not required
    ImmutableBitSet[] colBlocks; // primary key must be the first column block
    int rowBlockSize; // 0: disable row block
    ImmutableBitSet colBlocksAll;

    // must create from builder
    private GTInfo() {
    }

    public String getTableName() {
        return tableName;
    }

    public int getColumnCount() {
        return nColumns;
    }

    public DataType getColumnType(int i) {
        return colTypes[i];
    }

    public int getColumnBlockCount() {
        return colBlocks.length;
    }

    public ImmutableBitSet getColumnBlock(int i) {
        return colBlocks[i];
    }

    public ImmutableBitSet getPrimaryKey() {
        return primaryKey;
    }

    public ImmutableBitSet getAllColumns() {
        return colAll;
    }

    public boolean isRowBlockEnabled() {
        return rowBlockSize > 0;
    }

    public int getRowBlockSize() {
        return rowBlockSize;
    }

    public int getMaxRecordLength() {
        return getMaxColumnLength(colAll);
    }

    public int getMaxColumnLength(ImmutableBitSet selectedCols) {
        int result = 0;
        for (int i = 0; i < selectedCols.trueBitCount(); i++) {
            int c = selectedCols.trueBitAt(i);
            result += codeSystem.maxCodeLength(c);
        }
        return result;
    }

    public int getMaxColumnLength() {
        int max = 0;
        for (int i = 0; i < nColumns; i++)
            max = Math.max(max, codeSystem.maxCodeLength(i));
        return max;
    }

    public ImmutableBitSet selectColumns(ImmutableBitSet selectedColBlocks) {
        ImmutableBitSet result = ImmutableBitSet.EMPTY;
        for (int i = 0; i < selectedColBlocks.trueBitCount(); i++) {
            result = result.or(colBlocks[selectedColBlocks.trueBitAt(i)]);
        }
        return result;
    }

    public ImmutableBitSet selectColumnBlocks(ImmutableBitSet columns) {
        if (columns == null)
            columns = colAll;

        BitSet result = new BitSet();
        for (int i = 0; i < colBlocks.length; i++) {
            ImmutableBitSet cb = colBlocks[i];
            if (cb.intersects(columns)) {
                result.set(i);
            }
        }
        return new ImmutableBitSet(result);
    }

    public TblColRef colRef(int i) {
        if (colRefs == null) {
            colRefs = new TblColRef[nColumns];
        }
        if (colRefs[i] == null) {
            colRefs[i] = GTUtil.tblColRef(i, colTypes[i].toString());
        }
        return colRefs[i];
    }

    public void validateColRef(TblColRef ref) {
        TblColRef expected = colRef(ref.getColumnDesc().getZeroBasedIndex());
        if (!expected.equals(ref))
            throw new IllegalArgumentException();
    }

    void validate() {
        if (codeSystem == null)
            throw new IllegalStateException();

        if (primaryKey == null || primaryKey.cardinality() == 0)
            throw new IllegalStateException();

        codeSystem.init(this);

        validateColumnBlocks();
    }

    private void validateColumnBlocks() {
        colAll = new ImmutableBitSet(0, nColumns);

        if (colBlocks == null) {
            colBlocks = new ImmutableBitSet[2];
            colBlocks[0] = primaryKey;
            colBlocks[1] = colAll.andNot(primaryKey);
        }

        colBlocksAll = new ImmutableBitSet(0, colBlocks.length);

        if (colPreferIndex == null)
            colPreferIndex = ImmutableBitSet.EMPTY;

        // column blocks must not overlap
        for (int i = 0; i < colBlocks.length; i++) {
            for (int j = i + 1; j < colBlocks.length; j++) {
                if (colBlocks[i].intersects(colBlocks[j]))
                    throw new IllegalStateException();
            }
        }

        // column block must cover all columns
        ImmutableBitSet merge = ImmutableBitSet.EMPTY;
        for (int i = 0; i < colBlocks.length; i++) {
            merge = merge.or(colBlocks[i]);
        }
        if (!merge.equals(colAll))
            throw new IllegalStateException();

        // primary key must be the first column block
        if (!primaryKey.equals(colBlocks[0]))
            throw new IllegalStateException();

        // drop empty column block
        LinkedList<ImmutableBitSet> list = new LinkedList<ImmutableBitSet>(Arrays.asList(colBlocks));
        Iterator<ImmutableBitSet> it = list.iterator();
        while (it.hasNext()) {
            ImmutableBitSet cb = it.next();
            if (cb.isEmpty())
                it.remove();
        }
        colBlocks = list.toArray(new ImmutableBitSet[list.size()]);
    }

    public static class Builder {
        final GTInfo info;

        private Builder() {
            this.info = new GTInfo();
        }

        /** optional */
        public Builder setTableName(String name) {
            info.tableName = name;
            return this;
        }

        /** required */
        public Builder setCodeSystem(IGTCodeSystem cs) {
            info.codeSystem = cs;
            return this;
        }

        /** required */
        public Builder setColumns(DataType... colTypes) {
            info.nColumns = colTypes.length;
            info.colTypes = colTypes;
            return this;
        }

        /** required */
        public Builder setPrimaryKey(ImmutableBitSet primaryKey) {
            info.primaryKey = primaryKey;
            return this;
        }

        /** optional */
        public Builder enableColumnBlock(ImmutableBitSet[] columnBlocks) {
            info.colBlocks = new ImmutableBitSet[columnBlocks.length];
            for (int i = 0; i < columnBlocks.length; i++) {
                info.colBlocks[i] = columnBlocks[i];
            }
            return this;
        }

        /** optional */
        public Builder enableRowBlock(int rowBlockSize) {
            info.rowBlockSize = rowBlockSize;
            return this;
        }

        /** optional */
        public Builder setColumnPreferIndex(ImmutableBitSet colPreferIndex) {
            info.colPreferIndex = colPreferIndex;
            return this;
        }

        public GTInfo build() {
            info.validate();
            return info;
        }
    }

    public IGTCodeSystem getCodeSystem() {
        return codeSystem;
    }

    public int getMaxLength() {
        int ret = 0;
        for (int i = 0; i < colAll.trueBitCount(); i++) {
            ret += codeSystem.maxCodeLength(colAll.trueBitAt(i));
        }
        return ret;
    }

    public static final BytesSerializer<GTInfo> serializer = new BytesSerializer<GTInfo>() {
        @Override
        public void serialize(GTInfo value, ByteBuffer out) {
            if (value.codeSystem instanceof CubeCodeSystem) {
                BytesUtil.writeAsciiString(CubeCodeSystem.class.getCanonicalName(), out);
                TrimmedCubeCodeSystem trimmed = ((CubeCodeSystem) value.codeSystem).trimForCoprocessor();
                TrimmedCubeCodeSystem.serializer.serialize(trimmed, out);
            } else if (value.codeSystem != null) {
                BytesUtil.writeAsciiString(value.codeSystem.getClass().getCanonicalName(), out);
                BytesSerializer<IGTCodeSystem> serializer = null;
                try {
                    serializer = (BytesSerializer<IGTCodeSystem>) value.codeSystem.getClass().getField("serializer").get(null);
                } catch (IllegalAccessException | NoSuchFieldException e) {
                    throw new RuntimeException("failed to get serializer for " + value.codeSystem.getClass(), e);
                }
                serializer.serialize(value.codeSystem, out);
            } else {
                throw new IllegalStateException("code system cannot be null");
            }

            BytesUtil.writeUTFString(value.tableName, out);
            BytesUtil.writeVInt(value.colTypes.length, out);
            for (DataType dataType : value.colTypes) {
                DataType.serializer.serialize(dataType, out);
            }
            ImmutableBitSet.serializer.serialize(value.colPreferIndex, out);
            ImmutableBitSet.serializer.serialize(value.primaryKey, out);
            BytesUtil.writeVInt(value.colBlocks.length, out);
            for (ImmutableBitSet x : value.colBlocks) {
                ImmutableBitSet.serializer.serialize(x, out);
            }
            BytesUtil.writeVInt(value.rowBlockSize, out);
        }

        @Override
        public GTInfo deserialize(ByteBuffer in) {
            IGTCodeSystem codeSystem = null;
            String codeSystemType = BytesUtil.readAsciiString(in);
            if (CubeCodeSystem.class.getCanonicalName().equals(codeSystemType)) {
                codeSystem = TrimmedCubeCodeSystem.serializer.deserialize(in);
            } else {
                try {
                    Class clazz = Class.forName(codeSystemType);
                    BytesSerializer<IGTCodeSystem> serializer = (BytesSerializer<IGTCodeSystem>) clazz.getField("serializer").get(null);
                    codeSystem = serializer.deserialize(in);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to deserialize IGTCodeSystem " + codeSystemType, e);
                }
            }

            String newTableName = BytesUtil.readUTFString(in);

            int colTypesSize = BytesUtil.readVInt(in);
            DataType[] newColTypes = new DataType[colTypesSize];
            for (int i = 0; i < colTypesSize; ++i) {
                newColTypes[i] = DataType.serializer.deserialize(in);
            }

            ImmutableBitSet newColPreferIndex = ImmutableBitSet.serializer.deserialize(in);
            ImmutableBitSet newPrimaryKey = ImmutableBitSet.serializer.deserialize(in);

            int colBlockSize = BytesUtil.readVInt(in);
            ImmutableBitSet[] newColBlocks = new ImmutableBitSet[colBlockSize];
            for (int i = 0; i < colBlockSize; ++i) {
                newColBlocks[i] = ImmutableBitSet.serializer.deserialize(in);
            }

            int newRowBlockSize = BytesUtil.readVInt(in);

            return GTInfo.builder().setCodeSystem(codeSystem).//
            setTableName(newTableName).//
            setColumns(newColTypes).//
            setColumnPreferIndex(newColPreferIndex).//
            setPrimaryKey(newPrimaryKey).//
            enableColumnBlock(newColBlocks).//
            enableRowBlock(newRowBlockSize).build();
        }
    };

}
