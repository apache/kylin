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

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.cube.kv.RowKeyDecoderParquet;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.AbstractDateDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.FixedLenHexDimEnc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.basic.BigDecimalIngester;
import org.apache.kylin.measure.basic.DoubleIngester;
import org.apache.kylin.measure.basic.LongIngester;
import org.apache.kylin.metadata.datatype.BigDecimalSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Created by Yichen on 11/9/18.
 */
public class ParquetConvertor {
    private static final Logger logger = LoggerFactory.getLogger(ParquetConvertor.class);

    public static final String FIELD_CUBOID_ID = "cuboidId";
    public static final String DATATYPE_DECIMAL = "decimal";
    public static final String DATATYPE_INT = "int";
    public static final String DATATYPE_LONG = "long";
    public static final String DATATYPE_DOUBLE = "double";
    public static final String DATATYPE_STRING = "string";
    public static final String DATATYPE_BINARY = "binary";

    private RowKeyDecoder decoder;
    private BufferedMeasureCodec measureCodec;
    private Map<TblColRef, String> colTypeMap;
    private Map<MeasureDesc, String> meaTypeMap;
    private BigDecimalSerializer serializer;
    private GroupFactory factory;
    private List<MeasureDesc> measureDescs;

    public ParquetConvertor(String cubeName, String segmentId, KylinConfig kConfig, SerializableConfiguration sConf, Map<TblColRef, String> colTypeMap, Map<MeasureDesc, String> meaTypeMap){
        KylinConfig.setAndUnsetThreadLocalConfig(kConfig);

        this.colTypeMap = colTypeMap;
        this.meaTypeMap = meaTypeMap;
        serializer = new BigDecimalSerializer(DataType.getType(DATATYPE_DECIMAL));

        CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
        measureDescs = cubeDesc.getMeasures();
        decoder = new RowKeyDecoderParquet(cubeSegment);
        factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(sConf.get()));
        measureCodec = new BufferedMeasureCodec(cubeDesc.getMeasures());
    }

    protected Group parseValueToGroup(Text rawKey, Text rawValue) throws IOException{
        Group group = factory.newGroup();

        long cuboidId = decoder.decode(rawKey.getBytes());
        List<String> values = decoder.getValues();
        List<TblColRef> columns = decoder.getColumns();

        // for check
        group.append(FIELD_CUBOID_ID, cuboidId);

        for (int i = 0; i < columns.size(); i++) {
            TblColRef column = columns.get(i);
            parseColValue(group, column, values.get(i));
        }

        int[] valueLengths = measureCodec.getCodec().getPeekLength(ByteBuffer.wrap(rawValue.getBytes()));

        int valueOffset = 0;
        for (int i = 0; i < valueLengths.length; ++i) {
            MeasureDesc measureDesc = measureDescs.get(i);
            parseMeaValue(group, measureDesc, rawValue.getBytes(), valueOffset, valueLengths[i]);
            valueOffset += valueLengths[i];
        }

        return group;
    }

    private void parseColValue(final Group group, final TblColRef colRef, final String value) {
        if (value==null) {
            logger.error("value is null");
            return;
        }
        switch (colTypeMap.get(colRef)) {
            case DATATYPE_INT:
                group.append(colRef.getTableAlias() + "_" + colRef.getName(), Integer.valueOf(value));
                break;
            case DATATYPE_LONG:
                group.append(colRef.getTableAlias() + "_" + colRef.getName(), Long.valueOf(value));
                break;
            default:
                group.append(colRef.getTableAlias() + "_" + colRef.getName(), Binary.fromString(value));
                break;
        }
    }

    private void parseMeaValue(final Group group, final MeasureDesc measureDesc, final byte[] value, final int offset, final int length) throws IOException {
        if (value==null) {
            logger.error("value is null");
            return;
        }
        switch (meaTypeMap.get(measureDesc)) {
            case DATATYPE_LONG:
                group.append(measureDesc.getName(), BytesUtil.readVLong(ByteBuffer.wrap(value, offset, length)));
                break;
            case DATATYPE_DOUBLE:
                group.append(measureDesc.getName(), ByteBuffer.wrap(value, offset, length).getDouble());
                break;
            case DATATYPE_DECIMAL:
                BigDecimal decimal = serializer.deserialize(ByteBuffer.wrap(value, offset, length));
                decimal = decimal.setScale(4);
                group.append(measureDesc.getName(), Binary.fromByteArray(decimal.unscaledValue().toByteArray()));
                break;
            default:
                group.append(measureDesc.getName(), Binary.fromByteArray(value, offset, length));
                break;
        }
    }

    protected static MessageType cuboidToMessageType(Cuboid cuboid, IDimensionEncodingMap dimEncMap, CubeDesc cubeDesc) {
        Types.MessageTypeBuilder builder = Types.buildMessage();

        List<TblColRef> colRefs = cuboid.getColumns();

        builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(FIELD_CUBOID_ID);

        for (TblColRef colRef : colRefs) {
            DimensionEncoding dimEnc = dimEncMap.get(colRef);
            colToMessageType(dimEnc, colRef, builder);
        }

        MeasureIngester[] aggrIngesters = MeasureIngester.create(cubeDesc.getMeasures());

        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            MeasureDesc measureDesc = cubeDesc.getMeasures().get(i);
            DataType meaDataType = measureDesc.getFunction().getReturnDataType();
            MeasureType measureType = measureDesc.getFunction().getMeasureType();

            meaColToMessageType(measureType, measureDesc.getName(), meaDataType, aggrIngesters[i], builder);
        }

        return builder.named(String.valueOf(cuboid.getId()));
    }

    protected static void generateTypeMap(Cuboid cuboid, IDimensionEncodingMap dimEncMap, CubeDesc cubeDesc, Map<TblColRef, String> colTypeMap, Map<MeasureDesc, String> meaTypeMap){
        List<TblColRef> colRefs = cuboid.getColumns();

        for (TblColRef colRef : colRefs) {
            DimensionEncoding dimEnc = dimEncMap.get(colRef);
            addColTypeMap(dimEnc, colRef, colTypeMap);
        }

        MeasureIngester[] aggrIngesters = MeasureIngester.create(cubeDesc.getMeasures());

        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            MeasureDesc measureDesc = cubeDesc.getMeasures().get(i);
            MeasureType measureType = measureDesc.getFunction().getMeasureType();
            addMeaColTypeMap(measureType, measureDesc, aggrIngesters[i], meaTypeMap);
        }
    }
    private static String getColName(TblColRef colRef) {
        return colRef.getTableAlias() + "_" + colRef.getName();
    }

    private static void addColTypeMap(DimensionEncoding dimEnc, TblColRef colRef, Map<TblColRef, String> colTypeMap) {
        if (dimEnc instanceof AbstractDateDimEnc) {
            colTypeMap.put(colRef, DATATYPE_LONG);
        } else if (dimEnc instanceof FixedLenDimEnc || dimEnc instanceof FixedLenHexDimEnc) {
            DataType colDataType = colRef.getType();
            if (colDataType.isNumberFamily() || colDataType.isDateTimeFamily()){
                colTypeMap.put(colRef, DATATYPE_LONG);
            } else {
                // stringFamily && default
                colTypeMap.put(colRef, DATATYPE_STRING);
            }
        } else {
            colTypeMap.put(colRef, DATATYPE_INT);
        }
    }

    private static Map<MeasureDesc, String> addMeaColTypeMap(MeasureType measureType, MeasureDesc measureDesc, MeasureIngester aggrIngester, Map<MeasureDesc, String> meaTypeMap) {
        if (measureType instanceof BasicMeasureType) {
            MeasureIngester measureIngester = aggrIngester;
            if (measureIngester instanceof LongIngester) {
                meaTypeMap.put(measureDesc, DATATYPE_LONG);
            } else if (measureIngester instanceof DoubleIngester) {
                meaTypeMap.put(measureDesc, DATATYPE_DOUBLE);
            } else if (measureIngester instanceof BigDecimalIngester) {
                meaTypeMap.put(measureDesc, DATATYPE_DECIMAL);
            } else {
                meaTypeMap.put(measureDesc, DATATYPE_BINARY);
            }
        } else {
            meaTypeMap.put(measureDesc, DATATYPE_BINARY);
        }
        return meaTypeMap;
    }

    private static void colToMessageType(DimensionEncoding dimEnc, TblColRef colRef, Types.MessageTypeBuilder builder) {
        if (dimEnc instanceof AbstractDateDimEnc) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(getColName(colRef));
        } else if (dimEnc instanceof FixedLenDimEnc || dimEnc instanceof FixedLenHexDimEnc) {
            DataType colDataType = colRef.getType();
            if (colDataType.isNumberFamily() || colDataType.isDateTimeFamily()){
                builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(getColName(colRef));
            } else {
                // stringFamily && default
                builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(getColName(colRef));
            }
        } else {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(getColName(colRef));
        }
    }

    private static void meaColToMessageType(MeasureType measureType, String meaDescName, DataType meaDataType, MeasureIngester aggrIngester, Types.MessageTypeBuilder builder) {
        if (measureType instanceof BasicMeasureType) {
            MeasureIngester measureIngester = aggrIngester;
            if (measureIngester instanceof LongIngester) {
                builder.required(PrimitiveType.PrimitiveTypeName.INT64).named(meaDescName);
            } else if (measureIngester instanceof DoubleIngester) {
                builder.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(meaDescName);
            } else if (measureIngester instanceof BigDecimalIngester) {
                builder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.DECIMAL).precision(meaDataType.getPrecision()).scale(meaDataType.getScale()).named(meaDescName);
            } else {
                builder.required(PrimitiveType.PrimitiveTypeName.BINARY).named(meaDescName);
            }
        } else {
            builder.required(PrimitiveType.PrimitiveTypeName.BINARY).named(meaDescName);
        }
    }
}
