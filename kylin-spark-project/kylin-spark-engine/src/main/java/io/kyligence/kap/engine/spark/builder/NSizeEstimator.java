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

package io.kyligence.kap.engine.spark.builder;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Preconditions;

public class NSizeEstimator {
    public static long estimate(Dataset<Row> ds, float ratio) {
        if (ratio < 0.0001f)
            return 0;

        final int frequency = (int) (1 / ratio);

        StructType schema = new StructType();
        schema = schema.add("RowSize", DataTypes.LongType, false);
        List<Row> ret = ds.map(new MapFunction<Row, Row>() {
            private transient long count = 0;

            @Override
            public Row call(Row value) throws Exception {
                long size = 0;
                if (count % frequency == 0) {
                    for (int i = 0; i < value.size(); i++) {
                        size += value.get(i) == null ? 0 : value.get(i).toString().getBytes().length;
                    }
                }
                count++;
                return RowFactory.create(size);
            }
        }, org.apache.spark.sql.catalyst.encoders.RowEncoder.apply(schema)).agg(sum("RowSize")).collectAsList();

        Preconditions.checkArgument(ret.size() == 1);
        Row rowSizeStats = ret.get(0);
        if (rowSizeStats.isNullAt(0)) {
            return 0;
        }
        return rowSizeStats.getLong(0) * frequency;
    }

    public static Pair<Long, Long> estimate(Dataset<Row> ds, float ratio, SparkSession ss) {
        Column[] cols = new Column[ds.schema().size()];
        for (int i = 0; i < ds.schema().size(); i++) {
            cols[i] = new Column(ds.schema().fields()[i].name());
        }
        EstimateAggregateUdf udf = new EstimateAggregateUdf(ds.schema().size(), ratio);
        ss.udf().register(MetadataConstants.P_CUBOID_AGG_UDF, udf);
        List<Row> rows = ds.agg(callUDF(MetadataConstants.P_CUBOID_AGG_UDF, cols)).collectAsList();
        Row row = (Row) rows.get(0).get(0);
        return Pair.newPair(row.getLong(0), row.getLong(1));
    }

    private static class EstimateAggregateUdf extends UserDefinedAggregateFunction {
        private StructType inputSchema;
        private StructType bufferSchema;
        private DataType returnDataType;
        private int returnCount = 2;
        private int frequency;

        public EstimateAggregateUdf(int input, float ratio) {
            if (ratio < 0.0001f)
                frequency = 10000;

            frequency = (int) (1 / ratio);

            List<StructField> inputFields = new ArrayList<>();
            List<StructField> bufferFields = new ArrayList<>();
            List<StructField> returnFields = new ArrayList<>();

            for (int i = 0; i < input; i++) {
                StructField inputStructField = DataTypes.createStructField("input_items" + i, DataTypes.StringType,
                        true);
                inputFields.add(inputStructField);
            }

            for (int i = 0; i < returnCount; i++) {
                StructField bufferStructField = DataTypes.createStructField("buffer_items" + i, DataTypes.LongType,
                        true);
                bufferFields.add(bufferStructField);
            }

            for (int i = 0; i < returnCount; i++) {
                StructField returnStructField = DataTypes.createStructField("return_items" + i, DataTypes.LongType,
                        true);
                returnFields.add(returnStructField);
            }

            inputSchema = DataTypes.createStructType(inputFields);
            bufferSchema = DataTypes.createStructType(bufferFields);
            returnDataType = DataTypes.createStructType(returnFields);
        }

        @Override
        public StructType inputSchema() {
            return inputSchema;
        }

        @Override
        public StructType bufferSchema() {
            return bufferSchema;
        }

        @Override
        public DataType dataType() {
            return returnDataType;
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            for (int i = 0; i < returnCount; i++) {
                buffer.update(i, 0L);
            }
        }

        @Override
        public void update(MutableAggregationBuffer buffer, Row row) {
            long size = buffer.getLong(0);
            long count = buffer.getLong(1);
            if (count % frequency == 0) {
                for (int i = 0; i < row.size(); i++) {
                    if (row.get(i) == null)
                        continue;
                    size += row.getString(i).getBytes().length;
                }
            }
            count++;
            buffer.update(0, size);
            buffer.update(1, count);
        }

        @Override
        public void merge(MutableAggregationBuffer buffer, Row row) {
            buffer.update(0, row.getLong(0) + buffer.getLong(0));
            buffer.update(1, row.getLong(1) + buffer.getLong(1));
        }

        @Override
        public Object evaluate(Row row) {
            return row;
        }

    }
}
