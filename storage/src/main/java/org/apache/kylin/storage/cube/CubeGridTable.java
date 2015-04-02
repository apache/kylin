package org.apache.kylin.storage.cube;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GTDictionaryCodeSystem;
import org.apache.kylin.storage.gridtable.GTInfo;

import com.google.common.collect.Maps;

public class CubeGridTable {

    @SuppressWarnings("rawtypes")
    public static GTInfo newGTInfo(CubeDesc cubeDesc, long cuboidId, Map<TblColRef, Dictionary<?>> dictionaryMap) {
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        List<TblColRef> dimCols = cuboid.getColumns();
        
        int nColumns = dimCols.size() + cubeDesc.getMeasures().size();
        BitSet dimensions = new BitSet();
        dimensions.set(0, dimCols.size());
        BitSet metrics = new BitSet();
        metrics.set(dimCols.size(), nColumns);
        DataType[] dataTypes = new DataType[nColumns];
        Map<Integer, Dictionary> dictionaryByColIndex = Maps.newHashMap();

        int colIndex = 0;
        for (TblColRef col : dimCols) {
            dataTypes[colIndex] = col.getType();
            if (cubeDesc.getRowkey().isUseDictionary(col)) {
                Dictionary dict = dictionaryMap.get(col);
                if (dict == null)
                    throw new IllegalStateException();
                dictionaryByColIndex.put(colIndex, dict);
            }
            colIndex++;
        }

        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            dataTypes[colIndex] = measure.getFunction().getReturnDataType();
            colIndex++;
        }

        GTInfo.Builder builder = GTInfo.builder();
        builder.setCodeSystem(new GTDictionaryCodeSystem(dictionaryByColIndex));
        builder.setColumns(dataTypes);
        builder.setPrimaryKey(dimensions);
        builder.enableColumnBlock(new BitSet[] { dimensions, metrics });
        return builder.build();
    }


}
