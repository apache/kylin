package com.kylinolap.storage.hbase.coprocessor.endpoint;

import com.kylinolap.cube.invertedindex.Slice;
import com.kylinolap.metadata.model.realization.TblColRef;
import com.kylinolap.storage.filter.BitMapFilterEvaluator;
import com.kylinolap.storage.hbase.coprocessor.observer.ObserverRowType;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

/**
 * Created by Hongbin Ma(Binmahone) on 11/24/14.
 * <p/>
 * an adapter
 */
public class SliceBitMapProvider implements BitMapFilterEvaluator.BitMapProvider {

    private Slice slice;
    private ObserverRowType type;

    public SliceBitMapProvider(Slice slice, ObserverRowType type) {
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
