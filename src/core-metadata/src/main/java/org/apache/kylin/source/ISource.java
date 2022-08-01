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

package org.apache.kylin.source;

import java.io.Closeable;
import java.io.IOException;

import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;

/**
 * Represents a kind of source to Kylin, like Hive.
 */
public interface ISource extends Closeable {

    /**
     * Return an explorer to sync table metadata from the data source.
     */
    ISourceMetadataExplorer getSourceMetadataExplorer();

    /**
     * Return an adaptor that implements specified interface as requested by the build engine.
     * The IMRInput in particular, is required by the MR build engine.
     */
    <I> I adaptToBuildEngine(Class<I> engineInterface);

    /**
     * Return a ReadableTable that can iterate through the rows of given table.
     */
    IReadableTable createReadableTable(TableDesc tableDesc);

    /**
     * Give the source a chance to enrich a SourcePartition before build start.
     * Particularly, Kafka source use this chance to define start/end offsets within each partition.
     */
    SegmentRange enrichSourcePartitionBeforeBuild(IBuildable buildable, SegmentRange segmentRange);

    /**
     * Return an object that is responsible for deploying sample (CSV) data to the source database.
     * For testing purpose.
     */
    ISampleDataDeployer getSampleDataDeployer();

    SegmentRange getSegmentRange(String start, String end);

    @Override
    default void close() throws IOException {
        // just implement it
    }

    boolean supportBuildSnapShotByPartition();
}
