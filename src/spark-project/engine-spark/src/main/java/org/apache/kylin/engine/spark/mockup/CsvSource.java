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

package org.apache.kylin.engine.spark.mockup;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NSparkCubingEngine.NSparkCubingSource;
import org.apache.kylin.engine.spark.source.NSparkCubingSourceInput;
import org.apache.kylin.engine.spark.source.NSparkMetadataExplorer;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.utils.SchemaProcessor;
import org.apache.spark.sql.types.StructType;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class CsvSource implements ISource {

    // for reflection
    public CsvSource(KylinConfig config) {
    }

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new LocalSourceMetadataExplorer();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {

        if (engineInterface == NSparkCubingSource.class) {
            return (I) new NSparkCubingSource() {

                @Override
                public Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> parameters) {
                    if (KylinConfig.getInstanceFromEnv().getDDLLogicalViewDB()
                        .equalsIgnoreCase(table.getDatabase())) {
                      return new NSparkCubingSourceInput().getSourceData(table, ss, parameters);
                    }
                    String path = new File(getUtMetaDir(), "data/" + table.getIdentity() + ".csv").getAbsolutePath();
                    ColumnDesc[] columnDescs = table.getColumns();
                    List<ColumnDesc> tblColDescs = Lists.newArrayListWithCapacity(columnDescs.length);
                    for (ColumnDesc columnDesc : columnDescs) {
                        if (!columnDesc.isComputedColumn()) {
                            tblColDescs.add(columnDesc);
                        }
                    }
                    StructType structType = SchemaProcessor
                            .buildSchemaWithRawTable(tblColDescs.toArray(new ColumnDesc[0]));
                    return ss.read().option("delimiter", ",").schema(structType).csv(path);
                }
            };
        }
        throw new IllegalArgumentException("Unsupported engine interface: " + engineInterface);
    }

    @Override
    public IReadableTable createReadableTable(TableDesc tableDesc) {
        return new CsvTable(getUtMetaDir(), tableDesc);
    }

    @Override
    public SegmentRange enrichSourcePartitionBeforeBuild(IBuildable buildable, SegmentRange srcPartition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ISampleDataDeployer getSampleDataDeployer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SegmentRange getSegmentRange(String start, String end) {
        start = StringUtils.isEmpty(start) ? "0" : start;
        end = StringUtils.isEmpty(end) ? "" + Long.MAX_VALUE : end;
        return new SegmentRange.TimePartitionedSegmentRange(Long.parseLong(start), Long.parseLong(end));
    }

    private String getUtMetaDir() {
        // this is only meant to be used in UT
        final String utMetaDir = System.getProperty(KylinConfig.KYLIN_CONF);
        if (utMetaDir == null || !utMetaDir.startsWith("../example"))
            throw new IllegalStateException();
        return utMetaDir;
    }

    @Override
    public boolean supportBuildSnapShotByPartition() {
        return true;
    }

    private class LocalSourceMetadataExplorer implements ISourceMetadataExplorer {

        @Override
        public List<String> listDatabases() {
            Set<String> databases = new TreeSet<>();
            String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
            String path = resPath + "/../data/tableDesc";
            File[] files = new File(path).listFiles();
            for (File file : files) {
                if (!file.isDirectory()) {
                    String fileName = file.getName();
                    String suffix = fileName.substring(fileName.lastIndexOf(".") + 1);
                    if ("json".equals(suffix)) {
                        databases.add(fileName.substring(0, fileName.indexOf(".")));
                    }
                }
            }
            return new ArrayList<>(databases);
        }

        @Override
        public List<String> listTables(String database) {
            Set<String> tables = new TreeSet<>();
            String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
            String path = resPath + "/../data/tableDesc";
            File[] files = new File(path).listFiles();
            for (File file : files) {
                if (!file.isDirectory()) {
                    String fileName = file.getName();
                    String[] strings = fileName.split("\\.");
                    if (strings.length >= 1 && database.equals(strings[0])
                            && "json".equals(strings[strings.length - 1])) {
                        tables.add(strings[1]);
                    }
                }
            }
            return new ArrayList<>(tables);
        }

        @Override
        public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj)
            throws Exception {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            if (config.getDDLLogicalViewDB().equalsIgnoreCase(database)) {
                return new NSparkMetadataExplorer().loadTableMetadata(database, table, prj);
            }
            String resPath = config.getMetadataUrl().getIdentifier();
            String path = resPath + "/../data/tableDesc/" + database + "." + table + ".json";
            TableDesc tableDesc = JsonUtil.readValue(new File(path), TableDesc.class);
            for (ColumnDesc column : tableDesc.getColumns()) {
                column.setName(column.getName().toUpperCase(Locale.ROOT));
            }
            tableDesc.setTableType("defaultTable");
            tableDesc.init(prj);
            TableExtDesc tableExt = new TableExtDesc();
            tableExt.setIdentity(tableDesc.getIdentity());
            return Pair.newPair(tableDesc, tableExt);
        }

        @Override
        public List<String> getRelatedKylinResources(TableDesc table) {
            return Collections.emptyList();
        }

        @Override
        public boolean checkDatabaseAccess(String database) {
            return true;
        }

        @Override
        public boolean checkTablesAccess(Set<String> tables) {
            return true;
        }

        @Override
        public Set<String> getTablePartitions(String database, String table, String prj, String partitionCols) {
            if (table.equalsIgnoreCase("SUPPLIER") && partitionCols.equalsIgnoreCase("S_NATION")) {
                Set<String> partitions = Sets.newHashSet("2019-01-03", "2019-01-04");
                return partitions;
            }
            throw new UnsupportedOperationException();
        }

    }
}
