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

package org.apache.kylin.query.enumerator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.schema.OLAPTable;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Clarification(deprecated = true, msg = "Only for HBase storage")
public class LookupTableEnumerator implements Enumerator<Object[]> {
    private final static Logger logger = LoggerFactory.getLogger(LookupTableEnumerator.class);

//    private ILookupTable lookupTable;
    private final List<ColumnDesc> colDescs;
    private final Object[] current;
    private Iterator<String[]> iterator;

    public LookupTableEnumerator(OLAPContext olapContext) {

        //TODO: assuming LookupTableEnumerator is handled by a cube
        CubeInstance cube = null;

        if (olapContext.realization instanceof CubeInstance) {
            cube = (CubeInstance) olapContext.realization;
            ProjectInstance project = cube.getProjectInstance();
            List<RealizationEntry> realizationEntries = project.getRealizationEntries();
            String lookupTableName = olapContext.firstTableScan.getTableName();
            CubeManager cubeMgr = CubeManager.getInstance(cube.getConfig());

            // Make force hit cube in lookup table
            String forceHitCubeName = BackdoorToggles.getForceHitCube();
            if (!StringUtil.isEmpty(forceHitCubeName)) {
                String forceHitCubeNameLower = forceHitCubeName.toLowerCase(Locale.ROOT);
                String[] forceHitCubeNames = forceHitCubeNameLower.split(",");
                final Set<String> forceHitCubeNameSet = new HashSet<String>(Arrays.asList(forceHitCubeNames));
                cube = cubeMgr.findLatestSnapshot(
                        (List<RealizationEntry>) realizationEntries.stream()
                                .filter(x -> forceHitCubeNameSet.contains(x.getRealization().toLowerCase(Locale.ROOT))),
                        lookupTableName, cube);
                olapContext.realization = cube;
            } else {
                cube = cubeMgr.findLatestSnapshot(realizationEntries, lookupTableName, cube);
                olapContext.realization = cube;
            }
        } else if (olapContext.realization instanceof HybridInstance) {
            final HybridInstance hybridInstance = (HybridInstance) olapContext.realization;
            final IRealization latestRealization = hybridInstance.getLatestRealization();
            if (latestRealization instanceof CubeInstance) {
                cube = (CubeInstance) latestRealization;
            } else {
                throw new IllegalStateException();
            }
        }

        String lookupTableName = olapContext.firstTableScan.getTableName();
        DimensionDesc dim = cube.getDescriptor().findDimensionByTable(lookupTableName);
        if (dim == null)
            throw new IllegalStateException("No dimension with derived columns found for lookup table " + lookupTableName + ", cube desc " + cube.getDescriptor());

        CubeManager cubeMgr = CubeManager.getInstance(cube.getConfig());
//        this.lookupTable = cubeMgr.getLookupTable(cube.getLatestReadySegment(), dim.getJoin());

        OLAPTable olapTable = (OLAPTable) olapContext.firstTableScan.getOlapTable();
        this.colDescs = olapTable.getSourceColumns();
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
                    current[i] = Tuple.convertOptiqCellValue(row[colIdx], colDesc.getUpgradedType().getName());
                } else {
                    current[i] = null; // fake column
                }
            }
        }
        return hasNext;
    }

    @Override
    public Object[] current() {
        // NOTE if without the copy, sql_lookup/query03.sql will yields messy result. Very weird coz other lookup queries are all good.
        return Arrays.copyOf(current, current.length);
    }

    @Override
    public void reset() {
//        this.iterator = lookupTable.iterator();
    }

    @Override
    public void close() {
//        try {
//            lookupTable.close();
//        } catch (IOException e) {
//            logger.error("error when close lookup table", e);
//        }
    }

}
