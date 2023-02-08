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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.engine.spark.NSparkCubingEngine;

public class NSparkDataSource implements ISource {
    // for reflection
    public NSparkDataSource(KylinConfig config) {

    }

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new NSparkMetadataExplorer();
    }

    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == NSparkCubingEngine.NSparkCubingSource.class) {
            return (I) new NSparkCubingSourceInput();
        } else {
            throw new IllegalArgumentException("Unsupported engine interface: " + engineInterface);
        }
    }

    @Override
    public IReadableTable createReadableTable(TableDesc tableDesc) {
        return new NSparkTable(tableDesc);
    }

    @Override
    public SegmentRange enrichSourcePartitionBeforeBuild(IBuildable buildable, SegmentRange srcPartition) {
        return srcPartition;
    }

    @Override
    public ISampleDataDeployer getSampleDataDeployer() {
        return new NSparkMetadataExplorer();
    }

    @Override
    public SegmentRange getSegmentRange(String start, String end) {
        start = StringUtils.isEmpty(start) ? "0" : start;
        end = StringUtils.isEmpty(end) ? "" + Long.MAX_VALUE : end;
        return new SegmentRange.TimePartitionedSegmentRange(Long.parseLong(start), Long.parseLong(end));
    }

    @Override
    public boolean supportBuildSnapShotByPartition() {
        return true;
    }

}
