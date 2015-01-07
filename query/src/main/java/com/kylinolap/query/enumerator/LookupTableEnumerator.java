/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.query.enumerator;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.hydromatic.linq4j.Enumerator;

import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.dict.lookup.LookupStringTable;
import com.kylinolap.metadata.model.cube.DimensionDesc;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.schema.OLAPTable;
import com.kylinolap.storage.tuple.Tuple;

/**
 * @author yangli9
 * 
 */
public class LookupTableEnumerator implements Enumerator<Object[]> {

    private final Collection<String[]> allRows;
    private final List<ColumnDesc> colDescs;
    private final Object[] current;
    private Iterator<String[]> iterator;

    public LookupTableEnumerator(OLAPContext olapContext) {

        String lookupTableName = olapContext.firstTableScan.getCubeTable();
        DimensionDesc dim = olapContext.cubeDesc.findDimensionByTable(lookupTableName);
        if (dim == null)
            throw new IllegalStateException("No dimension with derived columns found for lookup table " + lookupTableName + ", cube desc " + olapContext.cubeDesc);

        CubeInstance cube = olapContext.cubeInstance;
        CubeManager cubeMgr = CubeManager.getInstance(cube.getConfig());
        LookupStringTable table = cubeMgr.getLookupTable(cube.getLatestReadySegment(), dim);
        this.allRows = table.getAllRows();

        OLAPTable olapTable = (OLAPTable) olapContext.firstTableScan.getOlapTable();
        this.colDescs = olapTable.getExposedColumns();
        this.current = new Object[colDescs.size()];

        reset();
    }

    @Override
    public boolean moveNext() {
        boolean hasNext = iterator.hasNext();
        if (hasNext) {
            String[] row = iterator.next();
            for (int i = 0, n = colDescs.size(); i < n; i++) {
                ColumnDesc colDesc = colDescs.get(i);
                int colIdx = colDesc.getZeroBasedIndex();
                if (colIdx >= 0) {
                    current[i] = Tuple.convertOptiqCellValue(row[colIdx], colDesc.getType().getName());
                } else {
                    current[i] = null; // fake column
                }
            }
        }
        return hasNext;
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public void reset() {
        this.iterator = allRows.iterator();
    }

    @Override
    public void close() {
    }

}
