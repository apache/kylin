/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube;

import com.google.common.base.Preconditions;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.engine.spark.metadata.cube.model.Cube;
import org.apache.kylin.engine.spark.metadata.cube.model.CubeUpdate2;
import org.apache.kylin.engine.spark.metadata.cube.model.DataModel;
import org.apache.kylin.engine.spark.metadata.cube.model.DataSegment;
import org.apache.kylin.engine.spark.metadata.cube.model.SegmentRange;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MetadataConverter {
    private MetadataConverter() {
    }

    public static Cube getCube(CubeInstance cubeInstance) {
        //TODO[xyxy]: add realization later
        Cube cube = Cube.getInstance(cubeInstance.getConfig());
        cube.setCubeName(cubeInstance.getName());
        cube.setDataModel(getDataModel(cubeInstance.getModel(),
                CubeDescManager.getInstance(cubeInstance.getConfig())
                        .getCubeDesc(cubeInstance.getDescName())));
        return null;
    }

    public static CubeInstance convertCube2CubeInstance(Cube cube) {
        //TODO[xyxy]: add realization later
        return null;
    }

    public static DataModel getDataModel(DataModelDesc dataModelDesc, CubeDesc cubeDesc) {
        DataModel dataModel = new DataModel(dataModelDesc.getConfig());

        dataModel.setRootFactTableName(dataModelDesc.getRootFactTableName());
       /* dataModel.setRootFactTable(dataModelDesc.getRootFactTable());
        dataModel.setJoinTables(Arrays.asList(dataModelDesc.getJoinTables()));
        dataModel.setAliasMap(dataModelDesc.getAliasMap());
        dataModel.setAllTables(dataModelDesc.getAllTables());
        dataModel.setPartitionDesc(dataModelDesc.getPartitionDesc());*/
        dataModel.setFilterCondition(dataModelDesc.getFilterCondition());
        dataModel.setAlias(dataModelDesc.getName());
        dataModel.setUuid(dataModelDesc.getUuid());

        // Cols => NameColumns
        List<DataModel.NamedColumn> allNamedColumns = new ArrayList<>();
        List<DataModel.Measure> measures = new ArrayList<>();
        int id = DataModel.DIMENSION_ID_BASE;
        for (DimensionDesc dimensionDesc : cubeDesc.getDimensions()) {
            DataModel.NamedColumn namedColumn = new DataModel.NamedColumn();
            namedColumn.setName(dimensionDesc.getName());
            namedColumn.setAliasDotColumn(dimensionDesc.getTable() + '.' +
                    dimensionDesc.getColumn());
            namedColumn.setStatus(DataModel.ColumnStatus.DIMENSION);
            namedColumn.setId(id++);
            allNamedColumns.add(namedColumn);
        }
        id = DataModel.MEASURE_ID_BASE;
        for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
            DataModel.NamedColumn namedColumn = new DataModel.NamedColumn();
            namedColumn.setId(id);
            namedColumn.setName(measureDesc.getName());
            namedColumn.setAliasDotColumn(measureDesc.getFunction().getParameter().getValue());
            namedColumn.setStatus(DataModel.ColumnStatus.EXIST);
            allNamedColumns.add(namedColumn);
            // Measures
            DataModel.Measure measure = new DataModel.Measure();
            measure.setName(measureDesc.getName());
            //measure.setFunction(measureDesc.getFunction());
            measure.setId(id++);
            measures.add(measure);
        }
        dataModel.setAllNamedColumns(allNamedColumns);
        dataModel.setAllMeasures(measures);

        dataModel.init();
        return dataModel;
    }

    public static CubeUpdate getCubeUpdate(CubeUpdate2 cubeUpdate) {
        //TODO[xyxy]: add realization later
        return null;
    }

    public static DataSegment getDataSegment(CubeSegment cubeSegment) {
        Preconditions
                .checkState(cubeSegment.getSegRange() instanceof org.apache.kylin.metadata.model.SegmentRange.TSRange);
        return new DataSegment(getCube(cubeSegment.getCubeInstance()),
                new SegmentRange.TimePartitionedSegmentRange(cubeSegment.getSegRange().start.toString(),
                        cubeSegment.getSegRange().end.toString()));
    }
}
