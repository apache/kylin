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

package org.apache.kylin.engine.spark.job;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.utils.SparkTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

public class ResourceDetectBeforeSampling extends SparkApplication {
    private static final Logger logger = LoggerFactory.getLogger(ResourceDetectBeforeSampling.class);

    @Override
    protected void doExecute() throws Exception {
        String tableName = getParam(MetadataConstants.TABLE_NAME);
        String project = getParam(MetadataConstants.P_PROJECT_NAME);
        final TableDesc tableDesc = TableMetadataManager.getInstance(config).getTableDesc(tableName, project);
        LinkedHashMap<String, String> params = ProjectManager.getInstance(config).getProject(project)
                .getOverrideKylinProps();
        long rowCount = Long.parseLong(getParam(MetadataConstants.TABLE_SAMPLE_MAX_COUNT));
        params.put("maxSampleCount", String.valueOf(rowCount));
        final Dataset<Row> dataset = getRowDataset(tableDesc);
        final List<Path> paths = JavaConversions
                .seqAsJavaList(ResourceDetectUtils.getPaths(dataset.queryExecution().sparkPlan()));

        Map<String, Long> resourceSize = Maps.newHashMap();
        resourceSize.put(String.valueOf(tableName), ResourceDetectUtils
                .getResourceSize(JavaConverters.asScalaIteratorConverter(paths.iterator()).asScala().toSeq()));

        Map<String, String> tableLeafTaskNums = Maps.newHashMap();
        tableLeafTaskNums.put(tableName, ResourceDetectUtils.getPartitions(dataset.queryExecution().executedPlan()));

        ResourceDetectUtils.write(
                new Path(config.getJobTmpShareDir(project, jobId), tableName + "_" + ResourceDetectUtils.fileName()),
                resourceSize);

        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                tableName + "_" + ResourceDetectUtils.samplingDetectItemFileSuffix()), tableLeafTaskNums);
    }

    private Dataset<Row> getRowDataset(TableDesc tableDesc) {
        ColumnDesc[] columns = tableDesc.getColumns();
        List<String> tblColNames = Lists.newArrayListWithCapacity(columns.length);
        StructType kylinSchema = new StructType();
        for (ColumnDesc columnDesc : columns) {
            if (!columnDesc.isComputedColumn()) {
                kylinSchema = kylinSchema.add(columnDesc.getName(),
                        SparkTypeUtil.toSparkType(columnDesc.getType(), false), true);
                tblColNames.add("`" + columnDesc.getName() + "`");
            }
        }
        String[] colNames = tblColNames.toArray(new String[0]);
        String colString = Joiner.on(",").join(colNames);
        String sql = String.format(Locale.ROOT, "select %s from %s", colString, tableDesc.getIdentity());
        Dataset<Row> df = ss.sql(sql);
        StructType sparkSchema = df.schema();
        logger.debug("Source data sql is: {}", sql);
        logger.debug("Kylin schema: {}", kylinSchema.treeString());
        return df.select(SparkTypeUtil.alignDataType(sparkSchema, kylinSchema));
    }

    public static void main(String[] args) {
        ResourceDetectBeforeSampling detect = new ResourceDetectBeforeSampling();
        detect.execute(args);
    }
}
