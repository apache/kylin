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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISourceMetadataExplorer;

import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.ByteType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.types.DataTypes.DateType;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used to read spark datasource table meta data.
 */
public class SparkDataSourceMetadataExplorer implements ISourceMetadataExplorer {
    private final Logger logger = LoggerFactory.getLogger(SparkDataSourceMetadataExplorer.class);
    private KylinConfig config = null;
    private Map<String, String> spark2KylinDataType = new HashMap<String, String>() {{
        // Not support ArrayType, ObjectType, NullType, StructType, UserDefinedType
        put(IntegerType.typeName(), "int");
        put(LongType.typeName(), "long");
        put(DoubleType.typeName(), "double");
        put(FloatType.typeName(), "float");
        put(ShortType.typeName(), "short");
        put(ByteType.typeName(), "byte");
        put(BooleanType.typeName(), "boolean");
        put(StringType.typeName(), "string");
        put(BinaryType.typeName(), "binary");
        put(TimestampType.typeName(), "timestamp");
        put(DateType.typeName(), "date");
        put("DecimalType", "decimal");
    }};

    public SparkDataSourceMetadataExplorer(KylinConfig config) {
        this.config = config;
        init();
    }

    private void init() {
        String defaultDb = config.getSparkSqlDefaultDatabase();
        for (String id: config.getSparkSqlSourceIds()) {
            String tableName = config.getSparkSqlDatasourceTableName(id);
            String[] arr = tableName.split("\\.");
            switch (arr.length) {
                case 1:
                    tableName = defaultDb + "." + tableName;
                    break;
                case 2:
                    break;
                default:
                    throw new RuntimeException("Illegal table name of spark datasource " +
                            id + ": " + tableName);
            }

            String className = config.getSparkSqlDataSourceClassName(id);
            List<String> paths = config.getSparkSqlDataSourcePaths(id);
            String schemaStr = config.getSparkSqlDataSourceSchema(id);
            List<String> partitionColumns = config.getSparkSqlDataSourcePartitionCols(id);
            String bucketSpecStr = config.getSparkSqlDataSourceBucketSpec(id);
            Map<String, String> options = config.getSparkSqlDatasourceOptions(id);

            SparkDataSourceDesc dataSourceDesc = new SparkDataSourceDesc(className, paths,
                    schemaStr, partitionColumns, bucketSpecStr, options);
            SparkDataSourceCache.add(tableName, dataSourceDesc);
            logger.info("Got spark datasource desc, id is " + id +
                    ", tableName is " + tableName + ", className is " + className);
        }
    }

    @Override
    public List<String> listDatabases() throws Exception {
        List<String> databases = Lists.newArrayList();
        databases.add(config.getSparkSqlDefaultDatabase());
        for (String table: SparkDataSourceCache.getTables()) {
            String db = table.split("\\.")[0];
            if (!databases.contains(db)) {
                databases.add(db);
            }
        }
        return databases;
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        List<String> tables = Lists.newArrayList();
        for (String table: SparkDataSourceCache.getTables()) {
            String[] arr = table.split("\\.");
            if (arr[0].equalsIgnoreCase(database)) {
                tables.add(arr[1]);
            }
        }
        return tables;
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database,
                                                           String table,
                                                           String prj) throws Exception {
        String tableIdentity = database + "." + table;
        TableDesc tableDesc = new TableDesc();
        SparkDataSourceDesc dataSourceDesc = SparkDataSourceCache.get(tableIdentity);
        DataSource dataSource = dataSourceDesc.toDataSource();

        tableDesc.setName(table);
        tableDesc.setDatabase(database);
        tableDesc.setProject(prj);
        tableDesc.setUuid(RandomUtil.randomUUID().toString());
        tableDesc.setTableType(dataSourceDesc.getClassName());
        ColumnDesc[] columnDescs = structType2ColumnDescs(tableDesc,
                dataSource.resolveRelation(false).schema());
        tableDesc.setColumns(columnDescs);
        tableDesc.setSourceType(ISourceAware.ID_SPARK_DATASOURCE);
        tableDesc.setBorrowedFromGlobal(false);

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableIdentity);
        tableExtDesc.setUuid(RandomUtil.randomUUID().toString());
        tableExtDesc.setLastModified(0);
        tableExtDesc.init(prj);

        tableExtDesc.addDataSourceProp("className", dataSourceDesc.getClassName());
        tableExtDesc.addDataSourceProp("paths",
                JsonUtil.writeValueAsString(dataSourceDesc.getPaths()));
        tableExtDesc.addDataSourceProp("schemaStr", dataSourceDesc.getSchemaStr());
        tableExtDesc.addDataSourceProp("partitionColumns",
                JsonUtil.writeValueAsString(dataSourceDesc.getPartitionColumns()));
        tableExtDesc.addDataSourceProp("bucketSpecStr", dataSourceDesc.getBucketSpecStr());
        tableExtDesc.addDataSourceProp("options",
                JsonUtil.writeValueAsString(dataSourceDesc.getOptions()));

        return new Pair<>(tableDesc, tableExtDesc);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnDesc[] evalQueryMetadata(String query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validateSQL(String query) throws Exception {
        throw new UnsupportedOperationException();
    }

    private ColumnDesc[] structType2ColumnDescs(TableDesc tableDesc, StructType structType) {
        int columnSize = structType.size();
        ColumnDesc[] columnDescArr = new ColumnDesc[columnSize];

        for (int i = 0; i < columnSize; i++) {
            StructField structField = structType.apply(i);
            String sparkSqlTypeName = structField.dataType().typeName();
            String kylinDataTypeName = spark2KylinDataType.get(sparkSqlTypeName);
            if (StringUtils.isEmpty(kylinDataTypeName)) {
                throw new RuntimeException("Can not load meta data of spark datasource table " +
                        tableDesc.getName() + ", because it contains unsupported type " +
                        sparkSqlTypeName);
            }

            ColumnDesc columnDesc = new ColumnDesc();
            columnDesc.setId(String.valueOf(i + 1));
            columnDesc.setDatatype(kylinDataTypeName);
            columnDesc.setName(structField.name());
            columnDesc.setNullable(structField.nullable());
            columnDesc.setTable(tableDesc);

            columnDescArr[i] = columnDesc;
        }

        return columnDescArr;
    }
}
