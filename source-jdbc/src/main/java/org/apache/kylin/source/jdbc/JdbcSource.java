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
package org.apache.kylin.source.jdbc;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.spark.ISparkInput;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourcePartition;

public class JdbcSource implements ISource {
    private final KylinConfig config;
    //used by reflection
    public JdbcSource(KylinConfig config) {
        this.config = config;
    }

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new JdbcExplorer(this.config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMRInput.class) {
            return (I) new JdbcHiveMRInput();
        } else if (engineInterface == ISparkInput.class) {
            return (I) new JdbcHiveSparkInput();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }

    @Override
    public IReadableTable createReadableTable(TableDesc tableDesc, String uuid) {
        return new JdbcTable(tableDesc);
    }

    @Override
    public SourcePartition enrichSourcePartitionBeforeBuild(IBuildable buildable, SourcePartition srcPartition) {
        SourcePartition result = SourcePartition.getCopyOf(srcPartition);
        result.setSegRange(null);
        return result;
    }

    @Override
    public ISampleDataDeployer getSampleDataDeployer() {
        return new JdbcExplorer(this.config);
    }

    @Override
    public void unloadTable(String tableName, String project) throws IOException {

    }

    @Override
    public void close() throws IOException {
        // not needed
    }
}
