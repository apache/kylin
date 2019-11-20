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

package org.apache.kylin.source.spark;

import java.util.List;
import java.util.Map;

import scala.Option;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.kylin.common.util.JsonUtil;

import org.apache.spark.sql.catalyst.catalog.BucketSpec;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents all the information needed to construct a Spark DataSource.
 */
public class SparkDataSourceDesc {
    private final Logger logger = LoggerFactory.getLogger(SparkDataSourceDesc.class);
    private String className;
    private List<String> paths = Lists.newArrayList();
    private String schemaStr;
    private List<String> partitionColumns = Lists.newArrayList();
    private String bucketSpecStr;
    private Map<String, String> options = Maps.newHashMap();

    public SparkDataSourceDesc(String className,
                               List<String> paths,
                               String schemaStr,
                               List<String> partitionColumns,
                               String bucketSpecStr,
                               Map<String, String> options) {
        this.className = className;
        this.paths = paths;
        this.schemaStr = schemaStr;
        this.partitionColumns = partitionColumns;
        this.bucketSpecStr = bucketSpecStr;
        this.options = options;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public String getSchemaStr() {
        return schemaStr;
    }

    public void setSchemaStr(String schemaStr) {
        this.schemaStr = schemaStr;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(List<String> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    public String getBucketSpecStr() {
        return bucketSpecStr;
    }

    public void setBucketSpecStr(String bucketSpecStr) {
        this.bucketSpecStr = bucketSpecStr;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    public DataSource toDataSource() {
        try {
            List<String> pathList = Lists.newArrayList();
            for (String path: paths) {
                pathList.add(path);
            }

            Option<StructType> schemaOption = Option.empty();
            if (StringUtils.isNotEmpty(schemaStr)) {
                schemaOption = Option.apply(jsonToStructType(schemaStr));
            }

            Option<BucketSpec> bucketSpecOption = Option.empty();
            if (StringUtils.isNotEmpty(bucketSpecStr)) {
                BucketSpec bucketSpec = JsonUtil.readValue(bucketSpecStr, BucketSpec.class);
                bucketSpecOption = Option.apply(bucketSpec);
            }

            return DataSource.apply(SparkSqlSource.sparkSession(), className,
                    ScalaUtils.javaListToScalaSeq(pathList), schemaOption,
                    ScalaUtils.javaListToScalaSeq(partitionColumns), bucketSpecOption,
                    ScalaUtils.javaMapToScalaMap(options), Option.empty());
        } catch (Exception e) {
            String exceptionMsg = "SparkDataSourceDesc transform to DataSource failed: "
                    + ExceptionUtils.getStackTrace(e);
            logger.error(exceptionMsg);
            throw new RuntimeException(exceptionMsg);
        }
    }

    private StructType jsonToStructType(String raw) {
        DataType dataType = DataType.fromJson(raw);
        if (dataType instanceof StructType) {
            return (StructType)dataType;
        } else {
            throw new RuntimeException("Failed parsing StructType: " + raw);
        }

    }
}
