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

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.spark.serializer.KryoRegistrator;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;

/**
 * Registor for registering classes and serializers to Kryo
 */
public class KylinKryoRegistrator implements KryoRegistrator {
    protected static final Logger logger = LoggerFactory.getLogger(KylinKryoRegistrator.class);

    @Override
    public void registerClasses(Kryo kryo) {

        Set<Class> kyroClasses = Sets.newLinkedHashSet();
        kyroClasses.add(byte[].class);
        kyroClasses.add(int[].class);
        kyroClasses.add(byte[][].class);
        kyroClasses.add(String[].class);
        kyroClasses.add(String[][].class);
        kyroClasses.add(Object[].class);
        kyroClasses.add(java.math.BigDecimal.class);
        kyroClasses.add(java.util.ArrayList.class);
        kyroClasses.add(java.util.LinkedList.class);
        kyroClasses.add(java.util.HashSet.class);
        kyroClasses.add(java.util.LinkedHashSet.class);
        kyroClasses.add(java.util.LinkedHashMap.class);
        kyroClasses.add(java.util.HashMap.class);
        kyroClasses.add(java.util.TreeMap.class);
        kyroClasses.add(java.util.Properties.class);
        kyroClasses.addAll(new Reflections("org.apache.kylin").getSubTypesOf(Serializable.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.dimension").getSubTypesOf(Serializable.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.cube").getSubTypesOf(Serializable.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.cube.model").getSubTypesOf(Object.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.metadata").getSubTypesOf(Object.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.metadata.model").getSubTypesOf(Object.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.metadata.measure").getSubTypesOf(Object.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.metadata.datatype").getSubTypesOf(org.apache.kylin.common.util.BytesSerializer.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.measure").getSubTypesOf(MeasureIngester.class));

        kyroClasses.add(org.apache.spark.sql.Row[].class);
        kyroClasses.add(org.apache.spark.sql.Row.class);
        kyroClasses.add(org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.class);
        kyroClasses.add(org.apache.spark.sql.types.StructType.class);
        kyroClasses.add(org.apache.spark.sql.types.StructField[].class);
        kyroClasses.add(org.apache.spark.sql.types.StructField.class);
        kyroClasses.add(org.apache.spark.sql.types.DateType$.class);
        kyroClasses.add(org.apache.spark.sql.types.Metadata.class);
        kyroClasses.add(org.apache.spark.sql.types.StringType$.class);
        kyroClasses.add(Hashing.murmur3_128().getClass());
        kyroClasses.add(org.apache.spark.sql.execution.columnar.CachedBatch.class);
        kyroClasses.add(org.apache.spark.sql.types.Decimal.class);
        kyroClasses.add(scala.math.BigDecimal.class);
        kyroClasses.add(java.math.MathContext.class);
        kyroClasses.add(java.math.RoundingMode.class);
        kyroClasses.add(java.util.concurrent.ConcurrentHashMap.class);
        kyroClasses.add(java.util.Random.class);
        kyroClasses.add(java.util.concurrent.atomic.AtomicLong.class);

        kyroClasses.add(org.apache.kylin.metadata.model.ColumnDesc[].class);
        kyroClasses.add(org.apache.kylin.metadata.model.JoinTableDesc[].class);
        kyroClasses.add(org.apache.kylin.metadata.model.TblColRef[].class);
        kyroClasses.add(org.apache.kylin.metadata.model.DataModelDesc.RealizationCapacity.class);
        kyroClasses.add(org.apache.kylin.metadata.model.DataModelDesc.TableKind.class);
        kyroClasses.add(org.apache.kylin.metadata.model.PartitionDesc.DefaultPartitionConditionBuilder.class);
        kyroClasses.add(org.apache.kylin.metadata.model.PartitionDesc.PartitionType.class);
        kyroClasses.add(org.apache.kylin.cube.model.CubeDesc.DeriveInfo.class);
        kyroClasses.add(org.apache.kylin.cube.model.CubeDesc.DeriveType.class);
        kyroClasses.add(org.apache.kylin.cube.model.HBaseColumnFamilyDesc[].class);
        kyroClasses.add(org.apache.kylin.cube.model.HBaseColumnDesc[].class);
        kyroClasses.add(org.apache.kylin.metadata.model.MeasureDesc[].class);
        kyroClasses.add(org.apache.kylin.cube.model.RowKeyColDesc[].class);
        kyroClasses.add(org.apache.kylin.common.util.Array.class);
        kyroClasses.add(org.apache.kylin.metadata.model.Segments.class);
        kyroClasses.add(org.apache.kylin.metadata.realization.RealizationStatusEnum.class);
        kyroClasses.add(org.apache.kylin.metadata.model.SegmentStatusEnum.class);
        kyroClasses.add(org.apache.kylin.measure.BufferedMeasureCodec.class);
        kyroClasses.add(org.apache.kylin.cube.kv.RowKeyColumnIO.class);
        kyroClasses.add(org.apache.kylin.measure.MeasureCodec.class);
        kyroClasses.add(org.apache.kylin.measure.MeasureAggregator[].class);
        kyroClasses.add(org.apache.kylin.metadata.datatype.DataTypeSerializer[].class);
        kyroClasses.add(org.apache.kylin.cube.kv.CubeDimEncMap.class);
        kyroClasses.add(org.apache.kylin.measure.basic.BasicMeasureType.class);
        kyroClasses.add(org.apache.kylin.common.util.SplittedBytes[].class);
        kyroClasses.add(org.apache.kylin.common.util.SplittedBytes.class);
        kyroClasses.add(org.apache.kylin.cube.kv.RowKeyEncoderProvider.class);
        kyroClasses.add(org.apache.kylin.cube.kv.RowKeyEncoder.class);
        kyroClasses.add(org.apache.kylin.measure.basic.BigDecimalIngester.class);
        kyroClasses.add(org.apache.kylin.dimension.DictionaryDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.IntDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.BooleanDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.DateDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.FixedLenDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.FixedLenHexDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.IntegerDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.OneMoreByteVLongDimEnc.class);
        kyroClasses.add(org.apache.kylin.dimension.TimeDimEnc.class);
        kyroClasses.add(org.apache.kylin.cube.model.AggregationGroup.HierarchyMask.class);
        kyroClasses.add(org.apache.kylin.measure.topn.DoubleDeltaSerializer.class);
        kyroClasses.add(org.apache.kylin.measure.bitmap.RoaringBitmapCounter.class);
        kyroClasses.add(org.roaringbitmap.buffer.MutableRoaringArray.class);
        kyroClasses.add(org.roaringbitmap.buffer.MappeableContainer[].class);
        kyroClasses.add(org.roaringbitmap.buffer.MutableRoaringBitmap.class);
        kyroClasses.add(org.roaringbitmap.buffer.MappeableArrayContainer.class);
        kyroClasses.add(org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory.class);
        kyroClasses.add(org.apache.kylin.measure.topn.Counter.class);
        kyroClasses.add(org.apache.kylin.measure.topn.TopNCounter.class);
        kyroClasses.add(org.apache.kylin.measure.percentile.PercentileSerializer.class);
        kyroClasses.add(com.tdunning.math.stats.AVLTreeDigest.class);
        kyroClasses.add(com.tdunning.math.stats.Centroid.class);

        addClassQuitely(kyroClasses, "com.google.common.collect.EmptyImmutableList");
        addClassQuitely(kyroClasses, "java.nio.HeapShortBuffer");
        addClassQuitely(kyroClasses, "scala.collection.immutable.Map$EmptyMap$");
        addClassQuitely(kyroClasses, "org.apache.spark.sql.catalyst.expressions.GenericInternalRow");
        addClassQuitely(kyroClasses, "org.apache.spark.unsafe.types.UTF8String");
        addClassQuitely(kyroClasses, "com.tdunning.math.stats.AVLGroupTree");

        for (Class kyroClass : kyroClasses) {
            kryo.register(kyroClass);
        }

        // TODO: should use JavaSerializer for PercentileCounter after Kryo bug be fixed: https://github.com/EsotericSoftware/kryo/issues/489
        //        kryo.register(PercentileCounter.class, new JavaSerializer());
    }

    private static void addClassQuitely(Set<Class> kyroClasses, String className) {
        try {
            kyroClasses.add(Class.forName(className));
        } catch (ClassNotFoundException e) {
            logger.error("failed to load class", e);
        }
    }
}
