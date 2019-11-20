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

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourcePartition;
import org.apache.kylin.source.ISampleDataDeployer;


/**
 * Represents spark datasource as the data source for kylin.
 */
public class SparkSqlSource implements ISource {
    private KylinConfig config = null;

    public static SparkSession sparkSession() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("spark in kylin");
        return SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
    }

    public SparkSqlSource(KylinConfig config) {
        this.config = config;
    }

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new SparkDataSourceMetadataExplorer(config);
    }

    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        return null;
    }

    @Override
    public IReadableTable createReadableTable(TableDesc tableDesc, String uuid) {
        return new SparkDataSourceTable(tableDesc);
    }

    @Override
    public SourcePartition enrichSourcePartitionBeforeBuild(IBuildable buildable,
                                                            SourcePartition srcPartition) {
        return srcPartition;
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
