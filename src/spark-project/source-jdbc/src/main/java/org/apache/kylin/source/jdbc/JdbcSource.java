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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_JDBC_SOURCE_CONFIG;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.engine.spark.NSparkCubingEngine;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSource implements ISource {

    private JdbcConnector dataSource;

    // for reflection
    public JdbcSource(KylinConfig config) {
        try {
            dataSource = SourceConnectorFactory.getJdbcConnector(config);
        } catch (Exception e) {
            log.error("DataSource cannot be connected.");
            throw new KylinException(INVALID_JDBC_SOURCE_CONFIG, MsgPicker.getMsg().getJdbcConnectionInfoWrong(), e);
        }
    }

    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == NSparkCubingEngine.NSparkCubingSource.class) {
            return (I) new JdbcSourceInput();
        } else {
            throw new IllegalArgumentException("Unsupported engine interface: " + engineInterface);
        }
    }

    @Override
    public IReadableTable createReadableTable(TableDesc tableDesc) {
        return new JdbcTable(dataSource, tableDesc);
    }

    @Override
    public SegmentRange enrichSourcePartitionBeforeBuild(IBuildable buildable, SegmentRange segmentRange) {
        return segmentRange;
    }

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new JdbcExplorer(dataSource);
    }

    @Override
    public ISampleDataDeployer getSampleDataDeployer() {
        return new JdbcExplorer(dataSource);
    }

    @Override
    public SegmentRange getSegmentRange(String start, String end) {
        start = StringUtils.isEmpty(start) ? "0" : start;
        end = StringUtils.isEmpty(end) ? "" + Long.MAX_VALUE : end;
        return new SegmentRange.TimePartitionedSegmentRange(Long.parseLong(start), Long.parseLong(end));
    }

    @Override
    public void close() throws IOException {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Override
    public boolean supportBuildSnapShotByPartition() {
        return false;
    }
}
