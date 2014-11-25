package com.kylinolap.storage.hbase.endpoint;

import com.kylinolap.cube.invertedindex.Slice;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.storage.filter.BitMapFilterEvaluator;
import com.kylinolap.storage.hbase.observer.SRowType;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

/**
 * Created by Hongbin Ma(Binmahone) on 11/24/14.
 * <p/>
 * an adapter
 */
public class SliceBitMapProvider implements BitMapFilterEvaluator.BitMapProvider {

    private Slice slice;
    private SRowType type;

    public SliceBitMapProvider(Slice slice, SRowType type) {
        this.slice = slice;
        this.type = type;
    }

    @Override
    public ConciseSet getBitMap(TblColRef col, int valueId) {
        return slice.getColumnValueContainer(type.getColIndexByTblColRef(col)).getBitMap(valueId);
    }

    @Override
    public int getRecordCount() {
        return this.slice.getRecordCount();
    }

    @Override
    public int getMaxValueId(TblColRef col) {
        return slice.getColumnValueContainer(type.getColIndexByTblColRef(col)).getMaxValueId();
    }
}
