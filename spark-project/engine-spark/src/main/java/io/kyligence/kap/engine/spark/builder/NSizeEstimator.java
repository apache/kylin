/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.builder;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.Pair;
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

import io.kyligence.kap.metadata.cube.model.NBatchConstants;

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
        ss.udf().register(NBatchConstants.P_CUBOID_AGG_UDF, udf);
        List<Row> rows = ds.agg(callUDF(NBatchConstants.P_CUBOID_AGG_UDF, cols)).collectAsList();
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
