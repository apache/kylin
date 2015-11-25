package org.apache.kylin.cube.gridtable;

import java.util.List;
import java.util.Map;

import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;

@SuppressWarnings("rawtypes")
public class CubeGridTable {

    public static Map<TblColRef, Dictionary<?>> getDimensionToDictionaryMap(CubeSegment cubeSeg, long cuboidId) {
        CubeDesc cubeDesc = cubeSeg.getCubeDesc();
        CubeManager cubeMgr = CubeManager.getInstance(cubeSeg.getCubeInstance().getConfig());

        // build a dictionary map
        Map<TblColRef, Dictionary<?>> dictionaryMap = Maps.newHashMap();
        List<TblColRef> dimCols = Cuboid.findById(cubeDesc, cuboidId).getColumns();
        for (TblColRef col : dimCols) {
            Dictionary<?> dictionary = cubeMgr.getDictionary(cubeSeg, col);
            if (dictionary != null) {
                dictionaryMap.put(col, dictionary);
            }
        }
        return dictionaryMap;
    }

    public static GTInfo newGTInfo(CubeSegment cubeSeg, long cuboidId) throws NotEnoughGTInfoException {
        Map<TblColRef, Dictionary<?>> dictionaryMap = getDimensionToDictionaryMap(cubeSeg, cuboidId);
        Cuboid cuboid = Cuboid.findById(cubeSeg.getCubeDesc(), cuboidId);
        for (TblColRef dim : cuboid.getColumns()) {
            if (cubeSeg.getCubeDesc().getRowkey().isUseDictionary(dim)) {
                Dictionary dict = dictionaryMap.get(dim);
                if (dict == null) {
                    throw new NotEnoughGTInfoException();
                }
            }
        }

        return newGTInfo(cubeSeg.getCubeDesc(), cuboidId, dictionaryMap);
    }

    public static GTInfo newGTInfo(CubeDesc cubeDesc, long cuboidId, Map<TblColRef, Dictionary<?>> dictionaryMap) {
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        CuboidToGridTableMapping mapping = new CuboidToGridTableMapping(cuboid);

        Map<Integer, Dictionary> dictionaryByColIdx = Maps.newHashMap();
        Map<Integer, Integer> fixLenByColIdx = Maps.newHashMap();

        for (TblColRef dim : cuboid.getColumns()) {
            int colIndex = mapping.getIndexOf(dim);
            if (cubeDesc.getRowkey().isUseDictionary(dim)) {
                Dictionary dict = dictionaryMap.get(dim);
                dictionaryByColIdx.put(colIndex, dict);
            } else {
                int len = cubeDesc.getRowkey().getColumnLength(dim);
                if (len == 0)
                    throw new IllegalStateException();

                fixLenByColIdx.put(colIndex, len);
            }
        }

        GTInfo.Builder builder = GTInfo.builder();
        builder.setTableName("Cuboid " + cuboidId);
        builder.setCodeSystem(new CubeCodeSystem(dictionaryByColIdx, fixLenByColIdx, mapping.getDependentMetricsMap()));
        builder.setColumns(mapping.getDataTypes());
        builder.setPrimaryKey(mapping.getPrimaryKey());
        builder.enableColumnBlock(mapping.getColumnBlocks());
        return builder.build();
    }
}
