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

package io.kyligence.kap.engine.spark.source;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;

public class NSparkDataSource implements ISource, IKeepNames {

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

}
