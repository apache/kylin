package org.apache.kylin.cube;

import org.apache.kylin.metadata.model.TblColRef;

import java.util.Collection;
import java.util.HashSet;

/**
 * Created by Hongbin Ma(Binmahone) on 1/8/15.
 *
 * the unified logic for defining a sql's dimension
 */
public class CubeDimensionDeriver {

    public static Collection<TblColRef> getDimensionColumns(Collection<TblColRef> groupByColumns, Collection<TblColRef> filterColumns) {
        Collection<TblColRef> dimensionColumns = new HashSet<TblColRef>();
        dimensionColumns.addAll(groupByColumns);
        dimensionColumns.addAll(filterColumns);
        return dimensionColumns;
    }
}
