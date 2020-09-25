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

package org.apache.kylin.engine.spark;

import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.kylin.engine.spark.util.PercentileCounterSerializer;
import org.apache.kylin.measure.percentile.PercentileCounter;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

/**
 * Registor for registering classes and serializers to Kryo
 */
public class KylinKryoRegistrator implements KryoRegistrator {
    protected static final Logger logger = LoggerFactory.getLogger(KylinKryoRegistrator.class);

    @Override
    public void registerClasses(Kryo kryo) {

        Set<Class> kyroClasses = new LinkedHashSet();
        kyroClasses.add(byte[].class);
        kyroClasses.add(int[].class);
        kyroClasses.add(byte[][].class);
        kyroClasses.add(String[].class);
        kyroClasses.add(String[][].class);
        kyroClasses.add(Object[].class);
        kyroClasses.add(Object.class);
        kyroClasses.add(Text.class);
        kyroClasses.add(java.math.BigDecimal.class);
        kyroClasses.add(java.util.ArrayList.class);
        kyroClasses.add(java.util.LinkedList.class);
        kyroClasses.add(java.util.HashSet.class);
        kyroClasses.add(java.util.LinkedHashSet.class);
        kyroClasses.add(java.util.LinkedHashMap.class);
        kyroClasses.add(java.util.HashMap.class);
        kyroClasses.add(java.util.TreeMap.class);
        kyroClasses.add(java.util.Properties.class);
        kyroClasses.add(java.math.MathContext.class);
        kyroClasses.add(java.math.RoundingMode.class);
        kyroClasses.add(java.util.concurrent.ConcurrentHashMap.class);
        kyroClasses.add(java.util.Random.class);
        kyroClasses.add(java.util.concurrent.atomic.AtomicLong.class);

        kyroClasses.add(org.apache.spark.sql.Row[].class);
        kyroClasses.add(org.apache.spark.sql.Row.class);
        kyroClasses.add(org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.class);
        kyroClasses.add(org.apache.spark.sql.types.StructType.class);
        kyroClasses.add(org.apache.spark.sql.types.StructField[].class);
        kyroClasses.add(org.apache.spark.sql.types.StructField.class);
        kyroClasses.add(org.apache.spark.sql.types.DateType$.class);
        kyroClasses.add(org.apache.spark.sql.types.Metadata.class);
        kyroClasses.add(org.apache.spark.sql.types.StringType$.class);
        kyroClasses.add(org.apache.spark.sql.execution.columnar.CachedBatch.class);
        kyroClasses.add(org.apache.spark.sql.types.Decimal.class);
        kyroClasses.add(scala.math.BigDecimal.class);

        kyroClasses.add(org.apache.kylin.common.util.SplittedBytes[].class);
        kyroClasses.add(org.apache.kylin.metadata.model.ColumnDesc[].class);
        kyroClasses.add(org.apache.kylin.metadata.model.JoinTableDesc[].class);
        kyroClasses.add(org.apache.kylin.metadata.model.TblColRef[].class);
        kyroClasses.add(org.apache.kylin.metadata.model.MeasureDesc[].class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.DataTypeSerializer[].class);
        kyroClasses.add(org.apache.kylin.measure.MeasureAggregator[].class);
        kyroClasses.add(org.apache.kylin.cube.model.HBaseColumnFamilyDesc[].class);
        kyroClasses.add(org.apache.kylin.cube.model.HBaseColumnDesc[].class);
        kyroClasses.add(org.apache.kylin.cube.model.RowKeyColDesc[].class);
        kylinClassByReflection1(kyroClasses);
        kylinClassByReflection2(kyroClasses);

        kyroClasses.add(org.apache.kylin.shaded.com.google.common.hash.Hashing.murmur3_128().getClass());
        kyroClasses.add(org.roaringbitmap.buffer.MutableRoaringArray.class);
        kyroClasses.add(org.roaringbitmap.buffer.MappeableContainer[].class);
        kyroClasses.add(org.roaringbitmap.buffer.MutableRoaringBitmap.class);
        kyroClasses.add(org.roaringbitmap.buffer.MappeableArrayContainer.class);
        kyroClasses.add(org.roaringbitmap.buffer.MappeableBitmapContainer.class);

        kyroClasses.add(Class.class);

        //shaded classes
        addClassQuitely(kyroClasses, "org.apache.kylin.job.shaded.org.roaringbitmap.buffer.MutableRoaringArray");
        addClassQuitely(kyroClasses, "org.apache.kylin.job.shaded.org.roaringbitmap.buffer.MutableRoaringBitmap");
        addClassQuitely(kyroClasses, "org.apache.kylin.job.shaded.org.roaringbitmap.buffer.MappeableArrayContainer");
        addClassQuitely(kyroClasses, "org.apache.kylin.job.shaded.org.roaringbitmap.buffer.MappeableBitmapContainer");
        addClassQuitely(kyroClasses, "org.apache.kylin.job.shaded.org.roaringbitmap.buffer.ImmutableRoaringBitmap");
        addClassQuitely(kyroClasses, "org.apache.kylin.job.shaded.org.roaringbitmap.buffer.ImmutableRoaringArray");
        addClassQuitely(kyroClasses, "org.apache.kylin.job.shaded.org.roaringbitmap.buffer.MappeableRunContainer");

        addClassQuitely(kyroClasses, "org.apache.kylin.shaded.com.google.common.collect.EmptyImmutableList");
        addClassQuitely(kyroClasses, "java.nio.HeapShortBuffer");
        addClassQuitely(kyroClasses, "java.nio.HeapLongBuffer");
        addClassQuitely(kyroClasses, "scala.collection.immutable.Map$EmptyMap$");
        addClassQuitely(kyroClasses, "org.apache.spark.sql.catalyst.expressions.GenericInternalRow");
        addClassQuitely(kyroClasses, "org.apache.spark.unsafe.types.UTF8String");

        addClassQuitely(kyroClasses, "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage");
        addClassQuitely(kyroClasses, "scala.collection.immutable.Set$EmptySet$");
        addClassQuitely(kyroClasses, "scala.reflect.ManifestFactory$$anon$2");
        addClassQuitely(kyroClasses, "scala.collection.mutable.WrappedArray$ofRef");

        for (Class kyroClass : kyroClasses) {
            kryo.register(kyroClass);
        }

        kryo.register(PercentileCounter.class, new PercentileCounterSerializer());
    }

    /**
     * Generated by KyroMappingGenerator, method exceeds 150 lines, splits into two methods
     * @param kyroClasses
     */
    private void kylinClassByReflection1(Set<Class> kyroClasses) {
        kyroClasses.add(org.apache.kylin.common.KylinConfig.class);
        kyroClasses.add(org.apache.kylin.common.KylinConfigBase.class);
        kyroClasses.add(org.apache.kylin.common.KylinConfigExt.class);
        kyroClasses.add(org.apache.kylin.common.persistence.RootPersistentEntity.class);
        kyroClasses.add(org.apache.kylin.common.util.Array.class);
        kyroClasses.add(org.apache.kylin.common.util.ByteArray.class);
        kyroClasses.add(org.apache.kylin.common.util.Dictionary.class);
        kyroClasses.add(org.apache.kylin.common.util.OrderedProperties.class);
        kyroClasses.add(org.apache.kylin.common.util.Pair.class);
        kyroClasses.add(org.apache.kylin.common.util.SplittedBytes.class);
        kyroClasses.add(org.apache.kylin.cube.CubeInstance.class);
        kyroClasses.add(org.apache.kylin.cube.CubeSegment.class);
        kyroClasses.add(org.apache.kylin.cube.common.RowKeySplitter.class);
        kyroClasses.add(org.apache.kylin.cube.cuboid.Cuboid.class);
        kyroClasses.add(org.apache.kylin.cube.cuboid.DefaultCuboidScheduler.class);
        kyroClasses.add(org.apache.kylin.cube.gridtable.TrimmedDimensionSerializer.class);
        kyroClasses.add(org.apache.kylin.cube.kv.AbstractRowKeyEncoder.class);
        kyroClasses.add(org.apache.kylin.cube.kv.CubeDimEncMap.class);
        kyroClasses.add(org.apache.kylin.cube.kv.FuzzyKeyEncoder.class);
        kyroClasses.add(org.apache.kylin.cube.kv.FuzzyMaskEncoder.class);
        kyroClasses.add(org.apache.kylin.cube.kv.LazyRowKeyEncoder.class);
        kyroClasses.add(org.apache.kylin.cube.kv.RowKeyColumnIO.class);
        kyroClasses.add(org.apache.kylin.cube.kv.RowKeyEncoder.class);
        kyroClasses.add(org.apache.kylin.cube.kv.RowKeyEncoderProvider.class);
        kyroClasses.add(org.apache.kylin.cube.model.AggregationGroup.class);
        kyroClasses.add(org.apache.kylin.cube.model.AggregationGroup.HierarchyMask.class);
        kyroClasses.add(org.apache.kylin.cube.model.CubeDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.CubeDesc.DeriveInfo.class);
        kyroClasses.add(org.apache.kylin.cube.model.CubeDesc.DeriveType.class);
        kyroClasses.add(org.apache.kylin.cube.model.CubeJoinedFlatTableDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich.class);
        kyroClasses.add(org.apache.kylin.cube.model.DictionaryDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.DimensionDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.HBaseColumnDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.HBaseColumnFamilyDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.HBaseMappingDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.HierarchyDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.RowKeyColDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.RowKeyDesc.class);
        kyroClasses.add(org.apache.kylin.cube.model.SelectRule.class);
        kyroClasses.add(org.apache.kylin.dict.AppendTrieDictionary.class);
        kyroClasses.add(org.apache.kylin.dict.CacheDictionary.class);
        kyroClasses.add(org.apache.kylin.dict.DateStrDictionary.class);
        kyroClasses.add(org.apache.kylin.dict.DictionaryInfo.class);
        kyroClasses.add(org.apache.kylin.dict.NumberDictionary.class);
        kyroClasses.add(org.apache.kylin.dict.NumberDictionary2.class);
        kyroClasses.add(org.apache.kylin.dict.Number2BytesConverter.class);
        kyroClasses.add(org.apache.kylin.dict.StringBytesConverter.class);
        kyroClasses.add(org.apache.kylin.dict.TimeStrDictionary.class);
        kyroClasses.add(org.apache.kylin.dict.TrieDictionary.class);
        kyroClasses.add(org.apache.kylin.dict.TrieDictionaryForest.class);
        kyroClasses.add(org.apache.kylin.dict.lookup.SnapshotTable.class);
        kyroClasses.add(org.apache.kylin.dimension.BooleanDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.BooleanDimEnc.BooleanSerializer.class);
        kyroClasses.add(org.apache.kylin.dimension.DateDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.DictionaryDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.DictionaryDimEnc.DictionarySerializer.class);
        kyroClasses.add(org.apache.kylin.dimension.FixedLenDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.FixedLenDimEnc.FixedLenSerializer.class);
        kyroClasses.add(org.apache.kylin.dimension.FixedLenHexDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.FixedLenHexDimEnc.FixedLenSerializer.class);
        kyroClasses.add(org.apache.kylin.dimension.IntDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.IntDimEnc.IntegerSerializer.class);
        kyroClasses.add(org.apache.kylin.dimension.IntegerDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.IntegerDimEnc.IntegerSerializer.class);
        kyroClasses.add(org.apache.kylin.dimension.OneMoreByteVLongDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.OneMoreByteVLongDimEnc.VLongSerializer.class);
        kyroClasses.add(org.apache.kylin.dimension.TimeDimEnc.class);
        kyroClasses.add(org.apache.kylin.engine.mr.common.BaseCuboidBuilder.class);
        kyroClasses.add(org.apache.kylin.engine.mr.common.NDCuboidBuilder.class);
        kyroClasses.add(org.apache.kylin.engine.spark.SparkCubingByLayer.class);
        kyroClasses.add(org.apache.kylin.job.JobInstance.class);
        kyroClasses.add(org.apache.kylin.job.dao.ExecutableOutputPO.class);
        kyroClasses.add(org.apache.kylin.job.dao.ExecutablePO.class);
    }

    /**
     * Generated by KyroMappingGenerator
     * @param kyroClasses
     */
    private void kylinClassByReflection2(Set<Class> kyroClasses) {
        kyroClasses.add(org.apache.kylin.measure.BufferedMeasureCodec.class);
        kyroClasses.add(org.apache.kylin.measure.MeasureAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.MeasureAggregators.class);
        kyroClasses.add(org.apache.kylin.measure.MeasureCodec.class);
        kyroClasses.add(org.apache.kylin.measure.MeasureIngester.class);
        kyroClasses.add(org.apache.kylin.measure.MeasureType.class);
        kyroClasses.add(org.apache.kylin.measure.MeasureTypeFactory.NeedRewriteOnlyMeasureType.class);
        kyroClasses.add(org.apache.kylin.measure.basic.BasicMeasureType.class);
        kyroClasses.add(org.apache.kylin.measure.basic.BigDecimalIngester.class);
        kyroClasses.add(org.apache.kylin.measure.basic.BigDecimalMaxAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.basic.BigDecimalMinAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.basic.BigDecimalSumAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.basic.DoubleIngester.class);
        kyroClasses.add(org.apache.kylin.measure.basic.DoubleMaxAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.basic.DoubleMinAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.basic.DoubleSumAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.basic.LongIngester.class);
        kyroClasses.add(org.apache.kylin.measure.basic.LongMaxAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.basic.LongMinAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.basic.LongSumAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.bitmap.BitmapAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.bitmap.BitmapMeasureType.class);
        kyroClasses.add(org.apache.kylin.measure.bitmap.BitmapSerializer.class);
        kyroClasses.add(org.apache.kylin.measure.bitmap.RoaringBitmapCounter.class);
        kyroClasses.add(org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory.class);
        kyroClasses.add(org.apache.kylin.measure.dim.DimCountDistinctMeasureType.class);
        kyroClasses.add(org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType.class);
        kyroClasses.add(org.apache.kylin.measure.extendedcolumn.ExtendedColumnSerializer.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.DenseRegister.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.HLLCAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.HLLCMeasureType.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.HLLCSerializer.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.HLLCounter.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.HLLCounterOld.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.HLLDistinctCountAggFunc.FixedValueHLLCMockup.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.HyperLogLogPlusTable.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.SingleValueRegister.class);
        kyroClasses.add(org.apache.kylin.measure.hllc.SparseRegister.class);
        kyroClasses.add(org.apache.kylin.measure.percentile.PercentileAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.percentile.PercentileMeasureType.class);
        kyroClasses.add(org.apache.kylin.measure.percentile.PercentileSerializer.class);
        kyroClasses.add(org.apache.kylin.measure.raw.RawAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.raw.RawMeasureType.class);
        kyroClasses.add(org.apache.kylin.measure.raw.RawSerializer.class);
        kyroClasses.add(org.apache.kylin.measure.topn.Counter.class);
        kyroClasses.add(org.apache.kylin.measure.topn.DoubleDeltaSerializer.class);
        kyroClasses.add(org.apache.kylin.measure.topn.TopNAggregator.class);
        kyroClasses.add(org.apache.kylin.measure.topn.TopNCounter.class);
        kyroClasses.add(org.apache.kylin.measure.topn.TopNCounterSerializer.class);
        kyroClasses.add(org.apache.kylin.measure.topn.TopNMeasureType.class);
        kyroClasses.add(org.apache.kylin.metadata.badquery.BadQueryEntry.class);
        kyroClasses.add(org.apache.kylin.metadata.badquery.BadQueryHistory.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.BigDecimalSerializer.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.BooleanSerializer.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.DataType.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.DataTypeSerializer.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.DateTimeSerializer.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.DoubleMutable.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.DoubleSerializer.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.Int4Serializer.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.IntMutable.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.Long8Serializer.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.LongMutable.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.LongSerializer.class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.StringSerializer.class);
        kyroClasses.add(org.apache.kylin.metadata.model.ColumnDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.DataModelDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.DataModelDesc.RealizationCapacity.class);
        kyroClasses.add(org.apache.kylin.metadata.model.DataModelDesc.TableKind.class);
        kyroClasses.add(org.apache.kylin.metadata.model.DatabaseDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.ExternalFilterDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.FunctionDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.JoinDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.JoinTableDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.JoinsTree.class);
        kyroClasses.add(org.apache.kylin.metadata.model.JoinsTree.Chain.class);
        kyroClasses.add(org.apache.kylin.metadata.model.MeasureDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.ModelDimensionDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.ParameterDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.PartitionDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.PartitionDesc.PartitionType.class);
        kyroClasses.add(org.apache.kylin.metadata.model.PartitionDesc.DefaultPartitionConditionBuilder.class);
        kyroClasses.add(org.apache.kylin.metadata.model.Segments.class);
        kyroClasses.add(org.apache.kylin.metadata.model.SegmentStatusEnum.class);
        kyroClasses.add(org.apache.kylin.metadata.model.TableDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.TableExtDesc.class);
        kyroClasses.add(org.apache.kylin.metadata.model.TableExtDesc.ColumnStats.class);
        kyroClasses.add(org.apache.kylin.metadata.model.TableRef.class);
        kyroClasses.add(org.apache.kylin.metadata.model.TblColRef.class);
        kyroClasses.add(org.apache.kylin.metadata.project.ProjectInstance.class);
        kyroClasses.add(org.apache.kylin.metadata.project.RealizationEntry.class);
        kyroClasses.add(org.apache.kylin.metadata.realization.RealizationStatusEnum.class);
        kyroClasses.add(org.apache.kylin.metadata.streaming.StreamingConfig.class);
        kyroClasses.add(org.apache.kylin.source.IReadableTable.TableSignature.class);
        kyroClasses.add(org.apache.kylin.storage.hybrid.HybridInstance.class);
    }

    private static void addClassQuitely(Set<Class> kyroClasses, String className) {
        try {
            kyroClasses.add(Class.forName(className));
        } catch (ClassNotFoundException e) {
            logger.error("failed to load class", e);
        }
    }
}
