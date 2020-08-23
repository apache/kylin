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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NSparkCubingEngine.NSparkCubingSource;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.engine.spark.metadata.TableDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourcePartition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvSource implements ISource {
    public CsvSource() {
    }

    //used by reflection
    public CsvSource(KylinConfig config) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        return (I) new NSparkCubingSource() {

            @Override
            public Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> parameters) {
                String path = null;
                KylinConfig kylinConfig = KylinBuildEnv.get().kylinConfig();
                String separator;
                boolean withHeader = false;
                if (kylinConfig.getDeployEnv().equals("UT")
                        && (parameters != null && parameters.get("separator") == null)) {
                    path = "file:///" + new File(getUtMetaDir(), "data/" + table.identity() + ".csv").getAbsolutePath();
                    separator = "";
                } else if (kylinConfig.getDeployEnv().equals("LOCAL")) {
                    path = "file:///" + new File(kylinConfig.getMetadataUrlPrefix(),
                            "data/" + table.identity() + ".csv")
                            .getAbsolutePath();
                    separator = "";
                } else {
                    String project = parameters.get("project") == null ? "" : parameters.get("project") + "/";
                    path = KylinBuildEnv.get().kylinConfig().getHdfsWorkingDirectory() + project + "csv/"
                            + table.identity() + ".csv";
                    separator = parameters.get("separator");
                    if (parameters.get("withHeader") != null) {
                        withHeader = Boolean.parseBoolean(parameters.get("withHeader"));
                    }
                }
                switch (separator) {
                case "space":
                    separator = " ";
                    break;
                case "tab":
                    separator = "\t";
                    break;
                default:
                    separator = ",";
                }

                Dataset<Row> delimiter = ss.read().option("delimiter", separator)
                        .option("header", withHeader).schema(table.toSchema()).csv(path);
                return delimiter;
            }
        };
    }

    @Override
    public SourcePartition enrichSourcePartitionBeforeBuild(IBuildable buildable, SourcePartition srcPartition) {
        SourcePartition result = SourcePartition.getCopyOf(srcPartition);
        result.setSegRange(null);
        return result;
    }

    private String getUtMetaDir() {
        // this is only meant to be used in UT
        final String utMetaDir = System.getProperty(KylinConfig.KYLIN_CONF);
        if (utMetaDir == null) {
            throw new IllegalStateException();
        }
        return utMetaDir;
    }

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return null;
    }

    @Override
    public IReadableTable createReadableTable(org.apache.kylin.metadata.model.TableDesc tableDesc, String uuid) {
        return null;
    }

    @Override
    public ISampleDataDeployer getSampleDataDeployer() {
        return null;
    }

    @Override
    public void unloadTable(String tableName, String project) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
