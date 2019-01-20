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
package org.apache.kylin.storage.parquet.steps;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.metadata.datatype.BigDecimalSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.parquet.NameMappingFactory;
import org.apache.kylin.storage.parquet.ParquetSchema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetConvertor {
    private static final Logger logger = LoggerFactory.getLogger(ParquetConvertor.class);

    private RowKeyDecoder decoder;
    private BufferedMeasureCodec measureCodec;
    private BigDecimalSerializer serializer = new BigDecimalSerializer(DataType.getType("decimal"));
    private GroupFactory factory;
    private ParquetSchema schema;

    public ParquetConvertor(String cubeName, String segmentId, KylinConfig kConfig, SerializableConfiguration sConf){
        KylinConfig.setAndUnsetThreadLocalConfig(kConfig);

        CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);

        decoder = new RowKeyDecoder(cubeSegment);
        factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(sConf.get()));
        measureCodec = new BufferedMeasureCodec(cubeDesc.getMeasures());
        schema = new ParquetSchema(NameMappingFactory.getDefault(cubeDesc), Cuboid.getBaseCuboid(cubeDesc).getColumns(), cubeDesc.getMeasures());
    }

    protected Group parseValueToGroup(Text rawKey, Text rawValue) throws IOException{
        Group group = factory.newGroup();

        long cuboidId = decoder.decode(rawKey.getBytes());
        List<String> values = decoder.getValues();
        List<TblColRef> columns = decoder.getColumns();

        // cuboid for check
        group.append(ParquetSchema.CUBOID_ID, cuboidId);

        // dimensions
        for (int i = 0; i < columns.size(); i++) {
            TblColRef column = columns.get(i);
            parseDimValue(group, column, values.get(i));
        }

        // measures
        int[] valueLengths = measureCodec.getCodec().getPeekLength(ByteBuffer.wrap(rawValue.getBytes()));

        int valueOffset = 0;
        for (int i = 0; i < valueLengths.length; ++i) {
            MeasureDesc measureDesc = schema.getMeasures().get(i);
            parseMeasureValue(group, measureDesc, rawValue.getBytes(), valueOffset, valueLengths[i]);
            valueOffset += valueLengths[i];
        }

        return group;
    }

    private void parseDimValue(final Group group, final TblColRef colRef, final String value) {
        if (value==null) {
            logger.error("value is null");
            return;
        }

        String colName = schema.getDimFieldName(colRef);

        switch (schema.getTypeByName(colName)) {
            case DATATYPE_BOOLEAN:
                group.append(colName, Boolean.valueOf(value));
                break;
            case DATATYPE_INT:
                group.append(colName, Integer.valueOf(value));
                break;
            case DATATYPE_LONG:
                group.append(colName, Long.valueOf(value));
                break;
            case DATATYPE_FLOAT:
                group.append(colName, Float.valueOf(value));
                break;
            case DATATYPE_DOUBLE:
                group.append(colName, Double.valueOf(value));
                break;
            case DATATYPE_DECIMAL:
                BigDecimal decimal = BigDecimal.valueOf(Long.valueOf(value));
                decimal.setScale(colRef.getType().getScale());
                group.append(colName, Binary.fromReusedByteArray(decimal.unscaledValue().toByteArray()));
                break;
            case DATATYPE_STRING:
                group.append(colName, value);
                break;
            case DATATYPE_BINARY:
                group.append(colName, Binary.fromString(value));
                break;
            default:
                group.append(colName, Binary.fromString(value));
        }
    }

    private void parseMeasureValue(final Group group, final MeasureDesc measureDesc, final byte[] value, final int offset, final int length) {
        if (value==null) {
            logger.error("value is null");
            return;
        }

        String meaName = schema.getMeasureFieldName(measureDesc);

        switch (schema.getTypeByName(meaName)) {
            case DATATYPE_INT:
                group.append(meaName, BytesUtil.readVInt(ByteBuffer.wrap(value, offset, length)));
                break;
            case DATATYPE_LONG:
                group.append(meaName,  BytesUtil.readVLong(ByteBuffer.wrap(value, offset, length)));
                break;
            case DATATYPE_FLOAT:
                group.append(meaName, ByteBuffer.wrap(value, offset, length).getFloat());
                break;
            case DATATYPE_DOUBLE:
                group.append(meaName, ByteBuffer.wrap(value, offset, length).getDouble());
                break;
            case DATATYPE_DECIMAL:
                BigDecimal decimal = serializer.deserialize(ByteBuffer.wrap(value, offset, length));
                decimal = decimal.setScale(measureDesc.getFunction().getReturnDataType().getScale());
                group.append(meaName, Binary.fromReusedByteArray(decimal.unscaledValue().toByteArray()));
                break;
            case DATATYPE_BINARY:
                group.append(meaName, Binary.fromReusedByteArray(value, offset, length));
                break;
            default:
                group.append(meaName, Binary.fromReusedByteArray(value, offset, length));
        }
    }
}
