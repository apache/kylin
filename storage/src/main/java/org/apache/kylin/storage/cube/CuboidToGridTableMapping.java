package org.apache.kylin.storage.cube;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CuboidToGridTableMapping {

    final private Cuboid cuboid;
    
    private List<DataType> gtDataTypes;
    private List<ImmutableBitSet> gtColBlocks;

    private int nDimensions;
    private Map<TblColRef, Integer> dim2gt;
    private ImmutableBitSet gtPrimaryKey;

    private int nMetrics;
    private ListMultimap<FunctionDesc, Integer> metrics2gt; // because count distinct may have a holistic version

    public CuboidToGridTableMapping(Cuboid cuboid) {
        this.cuboid = cuboid;
        init();
    }

    private void init() {
        int gtColIdx = 0;
        gtDataTypes = Lists.newArrayList();
        gtColBlocks = Lists.newArrayList();

        // dimensions
        dim2gt = Maps.newHashMap();
        BitSet pk = new BitSet();
        for (TblColRef dimension : cuboid.getColumns()) {
            gtDataTypes.add(dimension.getType());
            dim2gt.put(dimension, gtColIdx);
            pk.set(gtColIdx);
            gtColIdx++;
        }
        gtPrimaryKey = new ImmutableBitSet(pk);
        gtColBlocks.add(gtPrimaryKey);

        nDimensions = gtColIdx;
        assert nDimensions == cuboid.getColumns().size();

        // metrics
        metrics2gt = LinkedListMultimap.create();
        for (HBaseColumnFamilyDesc familyDesc : cuboid.getCube().getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc hbaseColDesc : familyDesc.getColumns()) {
                BitSet colBlock = new BitSet();
                for (MeasureDesc measure : hbaseColDesc.getMeasures()) {
                    // count distinct & holistic count distinct are equals() but different
                    // assert the holistic version if exists always comes earlier
                    FunctionDesc func = measure.getFunction();
                    if (func.isHolisticCountDistinct()) {
                        if (metrics2gt.get(func).size() > 0 )
                            throw new IllegalStateException();
                    }
                    gtDataTypes.add(func.getReturnDataType());
                    metrics2gt.put(func, gtColIdx);
                    colBlock.set(gtColIdx);
                    gtColIdx++;
                }
                gtColBlocks.add(new ImmutableBitSet(colBlock));
            }
        }
        nMetrics = gtColIdx - nDimensions;
        assert nMetrics == cuboid.getCube().getMeasures().size();
    }
    
    public int getColumnCount() {
        return nDimensions + nMetrics;
    }
    
    public int getDimensionCount() {
        return nDimensions;
    }
    
    public int getMetricsCount() {
        return nMetrics;
    }
    
    public DataType[] getDataTypes() {
        return (DataType[]) gtDataTypes.toArray(new DataType[gtDataTypes.size()]);
    }

    public ImmutableBitSet getPrimaryKey() {
        return gtPrimaryKey;
    }

    public ImmutableBitSet[] getColumnBlocks() {
        return (ImmutableBitSet[]) gtColBlocks.toArray(new ImmutableBitSet[gtColBlocks.size()]);
    }

    public int getIndexOf(TblColRef dimension) {
        Integer i = dim2gt.get(dimension);
        return i == null ? -1 : i.intValue();
    }

    public int getIndexOf(FunctionDesc metric) {
        List<Integer> list = metrics2gt.get(metric);
        // normal case
        if (list.size() == 1) {
            return list.get(0);
        }
        // count distinct & its holistic version
        else if (list.size() == 2) {
            assert metric.isCountDistinct();
            return metric.isHolisticCountDistinct() ? list.get(0) : list.get(1);
        }
        // unexpected
        else
            return -1;
    }

    public List<TblColRef> getCuboidDimensionsInGTOrder() {
        return cuboid.getColumns();
    }
}
