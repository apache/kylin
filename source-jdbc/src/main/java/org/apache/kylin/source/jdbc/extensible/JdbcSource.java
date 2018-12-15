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
package org.apache.kylin.source.jdbc.extensible;


import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.spark.ISparkInput;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSource implements ISource {
    private static final Logger logger = LoggerFactory.getLogger(JdbcSource.class);

    public static final int SOURCE_ID = 16;

    private JdbcConnector dataSource;

    //used by reflection
    public JdbcSource(KylinConfig config) {
        try {
            dataSource = SourceConnectorFactory.getJdbcConnector(config);
        } catch (Throwable e) {
            logger.warn("DataSource cannot be connected. This may not be required in a MapReduce job.", e);
        }
    }

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new JdbcExplorer(dataSource);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMRInput.class) {
            return (I) new JdbcHiveMRInput(dataSource);
        } else if (engineInterface == ISparkInput.class) {
            return (I) new JdbcHiveSparkInput(dataSource);
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }

    @Override
    public IReadableTable createReadableTable(TableDesc tableDesc, String uuid) {
        return new JdbcTable(dataSource, tableDesc);
    }

    @Override
    public SourcePartition enrichSourcePartitionBeforeBuild(IBuildable buildable, SourcePartition srcPartition) {
        SourcePartition result = SourcePartition.getCopyOf(srcPartition);
        result.setSegRange(null);
        return result;
    }

    @Override
    public ISampleDataDeployer getSampleDataDeployer() {
        return new JdbcExplorer(dataSource);
    }

    @Override
    public void unloadTable(String tableName, String project) throws IOException{
    }

    @Override
    public void close() throws IOException {
        if (dataSource != null)
            dataSource.close();
    }
}
