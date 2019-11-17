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

package io.kyligence.kap.engine.spark.mockup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.utils.SchemaProcessor;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NSparkCubingEngine.NSparkCubingSource;
import io.kyligence.kap.metadata.project.NProjectManager;

public class CsvSource implements ISource {

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new ISourceMetadataExplorer() {

            List<ProjectInstance> allProjects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .listAllProjects();

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
                return new ArrayList<String>(databases);
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
                return new ArrayList<String>(tables);
            }

            @Override
            public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj)
                    throws IOException {
                String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
                String path = resPath + "/../data/tableDesc/" + database + "." + table + ".json";
                TableDesc tableDesc = JsonUtil.readValue(new File(path), TableDesc.class);
                tableDesc.setTableType("defaultTable");
                TableExtDesc tableExt = new TableExtDesc();
                tableExt.setIdentity(tableDesc.getIdentity());
                return Pair.newPair(tableDesc, tableExt);
            }

            @Override
            public List<String> getRelatedKylinResources(TableDesc table) {
                return Collections.emptyList();
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {

        if (engineInterface == NSparkCubingSource.class) {
            return (I) new NSparkCubingSource() {

                @Override
                public Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> parameters) {

                    String path = new File(getUtMetaDir(), "data/" + table.getIdentity() + ".csv").getAbsolutePath();
                    ColumnDesc[] columnDescs = table.getColumns();
                    List<ColumnDesc> tblColDescs = Lists.newArrayListWithCapacity(columnDescs.length);
                    for (ColumnDesc columnDesc : columnDescs) {
                        if (!columnDesc.isComputedColumn()) {
                            tblColDescs.add(columnDesc);
                        }
                    }
                    StructType structType = SchemaProcessor.buildSchemaWithRawTable(tblColDescs.toArray(new ColumnDesc[0]));
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
}
