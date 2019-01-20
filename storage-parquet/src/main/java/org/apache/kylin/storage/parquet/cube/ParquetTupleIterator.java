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

package org.apache.kylin.storage.parquet.cube;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.measure.hllc.HLLCMeasureType;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.parquet.ParquetSchema;
import org.apache.spark.sql.Row;

import com.google.common.collect.Maps;

public class ParquetTupleIterator implements ITupleIterator {
    private final CubeInstance cubeInstance;
    private final StorageContext context;
    private final Iterator<Row> rowIterator;
    private final List<ColumnFiller> fillers;
    private int scanCount;
    private final Tuple tuple;
    private final int dimensionCount;
    private final int measureCount;
    private final Map<Integer, DataTypeSerializer> serializers;

    public ParquetTupleIterator(CubeInstance cubeInstance, StorageContext context, LookupTableCache lookupCache, ParquetSchema schema, TupleInfo tupleInfo, Iterator<Row> rowIterator) {
        this.cubeInstance = cubeInstance;
        this.context = context;
        this.rowIterator = rowIterator;
        this.tuple = new Tuple(tupleInfo);
        this.dimensionCount = schema.getDimensions().size();
        this.measureCount = schema.getMeasures().size();
        this.serializers = getSerializerMap(schema.getMeasures());

        this.fillers = new ArrayList<>();
        fillers.add(new ParquetColumnFiller(schema, tupleInfo));

        // derived columns fillers
        Map<Array<TblColRef>, List<CubeDesc.DeriveInfo>> hostToDerivedInfo = cubeInstance.getDescriptor().getHostToDerivedInfo(schema.getDimensions(), null);

        for (Map.Entry<Array<TblColRef>, List<CubeDesc.DeriveInfo>> entry : hostToDerivedInfo.entrySet()) {
            TblColRef[] hostCols = entry.getKey().data;
            for (CubeDesc.DeriveInfo info : entry.getValue()) {
                DerivedIndexMapping mapping = new DerivedIndexMapping(info, schema.getDimensions(), tupleInfo, hostCols);
                if (!mapping.shouldReturnDerived()) {
                    continue;
                }
                if (info.type == CubeDesc.DeriveType.LOOKUP) {
                    ILookupTable lookupTable = lookupCache.get(info.join);
                    fillers.add(new DerivedColumnFiller(mapping, lookupTable));

                } else if (info.type == CubeDesc.DeriveType.PK_FK) {
                    fillers.add(new PkColumnFiller(mapping));
                }
            }
        }
    }

    @Override
    public void close() {
        flushScanCountDelta();
    }

    @Override
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    @Override
    public ITuple next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (scanCount++ >= 100) {
            flushScanCountDelta();
            context.getQueryContext().checkMillisBeforeDeadline();
        }

        Object[] row = rowToObjects(rowIterator.next());
        for (ColumnFiller filler : fillers) {
            filler.fill(row, tuple);
        }
        return tuple;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    private void flushScanCountDelta() {
        context.increaseProcessedRowCount(scanCount);
        scanCount = 0;
    }

    private Object[] rowToObjects(Row row) {
        Object[] objects = new Object[dimensionCount + measureCount];
        for (int i = 0; i < dimensionCount; i++) {
            objects[i] = row.get(i);
        }

        for (int i = dimensionCount; i < dimensionCount + measureCount; i++) {
            objects[i] = serializers.get(i) == null ? row.get(i) : serializers.get(i).deserialize(ByteBuffer.wrap((byte[])row.get(i)));
        }

        return objects;
    }

    private Map<Integer, DataTypeSerializer> getSerializerMap(List<MeasureDesc> measures) {
        Map<Integer, DataTypeSerializer> serializerMap = Maps.newHashMap();
        int i = dimensionCount;
        for (MeasureDesc measure : measures) {
            MeasureType measureType = measure.getFunction().getMeasureType();
            if (measureType instanceof HLLCMeasureType ||
                    measureType instanceof BitmapMeasureType ||
                    measureType instanceof PercentileMeasureType ||
                    measureType instanceof ExtendedColumnMeasureType) {
                serializerMap.put(i, DataTypeSerializer.create(measure.getFunction().getReturnDataType()));
            }

            i++;
        }

        return serializerMap;
    }
}
