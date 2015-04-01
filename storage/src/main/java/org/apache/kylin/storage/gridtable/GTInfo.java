package org.apache.kylin.storage.gridtable;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.TblColRef;

public class GTInfo {

    public static Builder builder() {
        return new Builder();
    }

    IGTCodeSystem codeSystem;
    int maxRecordLength = 1024; // column length can vary

    // column schema
    int nColumns;
    DataType[] colTypes;
    BitSet colAll;
    BitSet colPreferIndex;
    transient TblColRef[] colRefs;

    // grid info
    BitSet primaryKey; // order by, uniqueness is not required
    BitSet[] colBlocks; // at least one column block
    BitSet colBlocksAll;
    int rowBlockSize; // 0: disable row block

    // sharding & rowkey
    int nShards; // 0: no sharding

    // must create from builder
    private GTInfo() {
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

    public BitSet selectColumnBlocks(BitSet columns) {
        if (columns == null)
            columns = colAll;

        BitSet result = new BitSet();
        for (int i = 0; i < colBlocks.length; i++) {
            BitSet cb = colBlocks[i];
            if (cb.intersects(columns)) {
                result.set(i);
            }
        }
        return result;
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
        TblColRef expected = colRef(ref.getColumn().getZeroBasedIndex());
        if (expected != ref)
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
        colAll = new BitSet();
        colAll.flip(0, nColumns);
        colBlocksAll = new BitSet();
        colBlocksAll.flip(0, colBlocks.length);

        if (colPreferIndex == null)
            colPreferIndex = new BitSet();

        // column blocks must not overlap
        for (int i = 0; i < colBlocks.length; i++) {
            for (int j = i + 1; j < colBlocks.length; j++) {
                if (colBlocks[i].intersects(colBlocks[j]))
                    throw new IllegalStateException();
            }
        }

        // column block must cover all columns
        BitSet merge = new BitSet();
        for (int i = 0; i < colBlocks.length; i++) {
            merge.or(colBlocks[i]);
        }
        if (merge.equals(colAll) == false)
            throw new IllegalStateException();

        // When row block is disabled, every row is treated as a row block, row PK is same as 
        // row block PK. Thus PK can be removed from column blocks. See <code>GTRowBlock</code>.
        if (isRowBlockEnabled() == false) {
            for (int i = 0; i < colBlocks.length; i++) {
                colBlocks[i].andNot(primaryKey);
            }
        }

        // drop empty column block
        LinkedList<BitSet> tmp = new LinkedList<BitSet>(Arrays.asList(colBlocks));
        Iterator<BitSet> it = tmp.iterator();
        while (it.hasNext()) {
            BitSet cb = it.next();
            if (cb.isEmpty())
                it.remove();
        }
        colBlocks = (BitSet[]) tmp.toArray(new BitSet[tmp.size()]);
    }

    public static class Builder {
        final GTInfo info;

        private Builder() {
            this.info = new GTInfo();
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
            if (info.colBlocks == null) {
                BitSet all = new BitSet();
                all.flip(0, info.nColumns);
                info.colBlocks = new BitSet[] { all };
            }
            return this;
        }

        /** required */
        public Builder setPrimaryKey(BitSet primaryKey) {
            info.primaryKey = (BitSet) primaryKey.clone();
            return this;
        }

        /** optional */
        public Builder setMaxRecordLength(int len) {
            info.maxRecordLength = len;
            return this;
        }

        /** optional */
        public Builder enableColumnBlock(BitSet[] columnBlocks) {
            info.colBlocks = new BitSet[columnBlocks.length];
            for (int i = 0; i < columnBlocks.length; i++) {
                info.colBlocks[i] = (BitSet) columnBlocks[i].clone();
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
        public Builder setColumnPreferIndex(BitSet colPreferIndex) {
            info.colPreferIndex = colPreferIndex;
            return this;
        }

        public GTInfo build() {
            info.validate();
            return info;
        }
    }

}
