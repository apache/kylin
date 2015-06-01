package org.apache.kylin.storage.gridtable;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.TblColRef;

public class GTInfo {

    public static Builder builder() {
        return new Builder();
    }

    String tableName;
    IGTCodeSystem codeSystem;

    // column schema
    int nColumns;
    DataType[] colTypes;
    ImmutableBitSet colAll;
    ImmutableBitSet colPreferIndex;
    transient TblColRef[] colRefs;

    // grid info
    ImmutableBitSet primaryKey; // order by, uniqueness is not required
    ImmutableBitSet[] colBlocks; // primary key must be the first column block
    ImmutableBitSet colBlocksAll;
    int rowBlockSize; // 0: disable row block

    // sharding & rowkey
    int nShards; // 0: no sharding

    // must create from builder
    private GTInfo() {
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public IGTCodeSystem getCodeSystem() {
        return codeSystem;
    }

    public int getColumnCount() {
        return nColumns;
    }
    
    public DataType getColumnType(int i) {
        return colTypes[i];
    }

    public ImmutableBitSet getPrimaryKey() {
        return primaryKey;
    }
    
    public boolean isShardingEnabled() {
        return nShards > 0;
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
        if (expected.equals(ref) == false)
            throw new IllegalArgumentException();
    }

    void validate() {

        if (codeSystem == null)
            throw new IllegalStateException();

        if (primaryKey.cardinality() == 0)
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
        if (merge.equals(colAll) == false)
            throw new IllegalStateException();

        // primary key must be the first column block
        if (primaryKey.equals(colBlocks[0]) == false)
            throw new IllegalStateException();

        // drop empty column block
        LinkedList<ImmutableBitSet> list = new LinkedList<ImmutableBitSet>(Arrays.asList(colBlocks));
        Iterator<ImmutableBitSet> it = list.iterator();
        while (it.hasNext()) {
            ImmutableBitSet cb = it.next();
            if (cb.isEmpty())
                it.remove();
        }
        colBlocks = (ImmutableBitSet[]) list.toArray(new ImmutableBitSet[list.size()]);
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
        public Builder enableSharding(int nShards) {
            info.nShards = nShards;
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

}
