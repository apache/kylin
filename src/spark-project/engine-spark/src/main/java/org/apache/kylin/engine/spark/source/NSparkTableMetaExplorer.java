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
package org.apache.kylin.engine.spark.source;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.jnet.Installer;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.kylin.common.constant.ObsConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.delta.DeltaTableUtils;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.collection.JavaConversions;

public class NSparkTableMetaExplorer implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkTableMetaExplorer.class);

    enum PROVIDER {
        HIVE("hive"), UNSPECIFIED("");

        private static final PROVIDER[] ALL = new PROVIDER[] { HIVE };
        private String value;

        PROVIDER(String value) {
            this.value = value;
        }

        public static PROVIDER fromString(Option<String> value) {
            if (value.isEmpty()) {
                return UNSPECIFIED;
            }

            for (PROVIDER provider : ALL) {
                if (provider.value.equals(value.get())) {
                    return provider;
                }
            }
            return UNSPECIFIED;
        }
    }

    private static final List<String> UNSUPOORT_TYPE = Lists.newArrayList("array", "map", "struct", "binary");
    private static final String CHAR_VARCHAR_TYPE_STRING_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING";

    public NSparkTableMeta getSparkTableMeta(String database, String tableName) {
        SessionCatalog catalog = SparderEnv.getSparkSession().sessionState().catalog();
        TableIdentifier tableIdentifier = TableIdentifier.apply(tableName,
                Option.apply(database.isEmpty() ? null : database));
        CatalogTable tableMetadata = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);
        checkTableIsValid(tableMetadata, tableIdentifier, tableName);
        return buildSparkTableMeta(tableName, tableMetadata);
    }

    public Set<String> checkAndGetTablePartitions(String database, String tableName, String partitionCol) {
        SessionCatalog catalog = SparderEnv.getSparkSession().sessionState().catalog();
        TableIdentifier tableIdentifier = TableIdentifier.apply(tableName,
                Option.apply(database.isEmpty() ? null : database));

        CatalogTable tableMetadata = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);

        String firstPartCol = tableMetadata.partitionColumnNames().isEmpty() ? null
                : tableMetadata.partitionColumnNames().head().toLowerCase(Locale.ROOT);

        if (!partitionCol.equalsIgnoreCase(firstPartCol)) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "table partition col %s not match col %s", firstPartCol, partitionCol));
        }
        return JavaConversions.seqAsJavaList(catalog.listPartitions(tableIdentifier, Option.empty())).stream()
                .map(item -> JavaConversions.mapAsJavaMap(item.spec()).entrySet().stream()
                        .filter(entry -> partitionCol.equalsIgnoreCase(entry.getKey())) //
                        .findFirst() //
                        .map(Map.Entry::getValue) //
                        .orElse(null))
                .filter(Objects::nonNull).collect(Collectors.toSet());
    }

    protected NSparkTableMeta buildSparkTableMeta(String tableName, CatalogTable tableMetadata) {
        NSparkTableMeta.NSparkTableMetaBuilder builder = NSparkTableMeta.builder();
        builder.tableName(tableName);
        StructType allColSchema = tableMetadata.schema();
        if (DeltaTableUtils.isDeltaTable(tableMetadata)) {
            allColSchema = SparderEnv.getSparkSession().table(tableMetadata.identifier()).schema();
        }
        builder.allColumns(getColumns(tableMetadata, allColSchema));
        builder.owner(tableMetadata.owner());
        builder.createTime(tableMetadata.createTime() + "");
        builder.lastAccessTime(tableMetadata.lastAccessTime() + "");
        builder.tableType(tableMetadata.tableType().name());
        builder.partitionColumns(getColumns(tableMetadata, tableMetadata.partitionSchema()));
        builder.isRangePartition(isRangePartition(tableMetadata));

        if (tableMetadata.comment().isDefined()) {
            builder.tableComment(tableMetadata.comment().get());
        }
        if (tableMetadata.storage().inputFormat().isDefined()) {
            builder.sdInputFormat(tableMetadata.storage().inputFormat().get());
        }
        if (tableMetadata.storage().outputFormat().isDefined()) {
            builder.sdOutputFormat(tableMetadata.storage().outputFormat().get());
        }
        Option<URI> uriOption = tableMetadata.storage().locationUri();
        ObsConfig obsConfig = ObsConfig.S3;
        if (uriOption.isDefined()) {
            builder.sdLocation(uriOption.get().toString());
            obsConfig = ObsConfig.getByLocation(uriOption.get().toString()).orElse(ObsConfig.S3);
        }
        if (tableMetadata.provider().isDefined()) {
            builder.provider(tableMetadata.provider().get());
        }
        if (tableMetadata.properties().contains("totalSize")) {
            builder.fileSize(Long.parseLong(tableMetadata.properties().get("totalSize").get()));
        }
        if (tableMetadata.properties().contains("numFiles")) {
            builder.fileNum(Long.parseLong(tableMetadata.properties().get("numFiles").get()));
        }
        if (tableMetadata.properties().contains("transactional")) {
            builder.isTransactional(Boolean.parseBoolean(tableMetadata.properties().get("transactional").get()));
        }
        if (tableMetadata.properties().contains(obsConfig.getRolePropertiesKey())) {
            builder.roleArn(tableMetadata.properties().get(obsConfig.getRolePropertiesKey()).get());
        }
        if (tableMetadata.properties().contains(obsConfig.getEndpointPropertiesKey())) {
            builder.endpoint(tableMetadata.properties().get(obsConfig.getEndpointPropertiesKey()).get());
        }
        if (tableMetadata.properties().contains(obsConfig.getRegionPropertiesKey())) {
            builder.region(tableMetadata.properties().get(obsConfig.getRegionPropertiesKey()).get());
        }
        builder.sourceType(ISourceAware.ID_SPARK);
        return builder.build();
    }

    private List<NSparkTableMeta.SparkTableColumnMeta> getColumns(CatalogTable tableMetadata, StructType schema) {
        return getColumns(tableMetadata, schema, true);
    }

    private List<NSparkTableMeta.SparkTableColumnMeta> getColumns(CatalogTable tableMetadata, StructType schema,
            boolean isCheckRepeatColumn) {
        List<NSparkTableMeta.SparkTableColumnMeta> allColumns = Lists.newArrayListWithCapacity(schema.size());
        Set<String> columnCacheTemp = Sets.newHashSet();
        for (org.apache.spark.sql.types.StructField field : schema.fields()) {
            String type = field.dataType().simpleString();
            if (field.metadata().contains(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)) {
                type = field.metadata().getString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY);
            }
            String finalType = type;
            if (UNSUPOORT_TYPE.stream().anyMatch(finalType::contains)) {
                logger.info("Load table {} ignore column {}:{}", tableMetadata.identifier().identifier(), field.name(),
                        finalType);
                continue;
            }
            if (isCheckRepeatColumn && columnCacheTemp.contains(field.name())) {
                logger.info("The【{}】column is already included and does not need to be added again", field.name());
                continue;
            }
            columnCacheTemp.add(field.name());
            allColumns.add(new NSparkTableMeta.SparkTableColumnMeta(field.name(), type,
                    field.getComment().isDefined() ? field.getComment().get() : null));
        }

        return allColumns;
    }

    private void checkTableIsValid(CatalogTable tableMetadata, TableIdentifier tableIdentifier, String tableName) {
        if (CatalogTableType.VIEW().equals(tableMetadata.tableType())) {
            try {
                Installer.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
                SparderEnv.getSparkSession().table(tableIdentifier).queryExecution().analyzed();
            } catch (Exception e) {
                throw new RuntimeException("Error for parser view: " + tableName + ", " + e.getMessage()
                        + "(There are maybe syntactic differences between HIVE and SparkSQL)", e);
            }
        }
    }

    private Boolean isRangePartition(CatalogTable tableMetadata) {
        List<NSparkTableMeta.SparkTableColumnMeta> allColumns = getColumns(tableMetadata, tableMetadata.schema(),
                false);
        return allColumns.stream().collect(Collectors.groupingBy(p -> p.name)).values().stream()
                .anyMatch(p -> p.size() > 1);
    }
}
