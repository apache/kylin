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
package io.kyligence.kap.engine.spark.source;

import java.util.List;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NSparkCubingSourceInput implements NSparkCubingEngine.NSparkCubingSource {
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingSourceInput.class);

    @Override
    public Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> getSourceData) {
        ColumnDesc[] columnDescs = table.getColumns();
        List<String> tblColNames = Lists.newArrayListWithCapacity(columnDescs.length);
        StructType kylinSchema = new StructType();
        for (ColumnDesc columnDesc : columnDescs) {
            if (!columnDesc.isComputedColumn()) {
                kylinSchema = kylinSchema.add(columnDesc.getName(), SparderTypeUtil.toSparkType(columnDesc.getType(), false), true);
                tblColNames.add(columnDesc.getName());
            }
        }
        String[] colNames = tblColNames.toArray(new String[0]);
        String colString = Joiner.on(",").join(colNames);
        String sql = String.format("select %s from %s", colString, table.getIdentity());
        Dataset<Row> df = ss.sql(sql);
        StructType sparkSchema = df.schema();
        logger.info("Source data sql is: " + sql);
        logger.info("Kylin schema " + kylinSchema.treeString());
        return df.select(SparderTypeUtil.alignDataType(sparkSchema, kylinSchema));
    }
}
