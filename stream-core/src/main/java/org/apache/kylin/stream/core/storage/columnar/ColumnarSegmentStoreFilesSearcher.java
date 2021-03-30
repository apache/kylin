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

package org.apache.kylin.stream.core.storage.columnar;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.stream.core.query.IStreamingGTSearcher;
import org.apache.kylin.stream.core.query.ResultCollector;
import org.apache.kylin.stream.core.query.ResultCollector.CloseListener;
import org.apache.kylin.stream.core.query.StreamingDataQueryPlanner;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("restriction")
public class ColumnarSegmentStoreFilesSearcher implements IStreamingGTSearcher {
    private static Logger logger = LoggerFactory.getLogger(ColumnarSegmentStoreFilesSearcher.class);

    private String segmentName;
    private List<DataSegmentFragment> fragments;
    private StreamingQueryProfile queryProfile;

    public ColumnarSegmentStoreFilesSearcher(String segmentName, List<DataSegmentFragment> fragments) {
        this.segmentName = segmentName;
        this.fragments = fragments;
        this.queryProfile = StreamingQueryProfile.get();
    }

    @Override
    public void search(final StreamingSearchContext searchContext, ResultCollector collector) throws IOException {
        logger.info("query-{}: scan segment {}, fragment files num:{}", queryProfile.getQueryId(),
                segmentName, fragments.size());
        for (DataSegmentFragment fragment : fragments) {
            File metaFile = fragment.getMetaFile();
            if (!metaFile.exists()) {
                if (queryProfile.isDetailProfileEnable()) {
                    logger.info("query-{}: for segment {} skip fragment {}, no meta file exists",
                            queryProfile.getQueryId(), segmentName, fragment.getFragmentId());
                }
                continue;
            }

            FragmentData fragmentData = loadFragmentData(fragment);
            FragmentMetaInfo fragmentMetaInfo = fragmentData.getFragmentMetaInfo();
            StreamingDataQueryPlanner queryPlanner = searchContext.getQueryPlanner();
            if (fragmentMetaInfo.hasValidEventTimeRange()
                    && queryPlanner.canSkip(fragmentMetaInfo.getMinEventTime(), fragmentMetaInfo.getMaxEventTime(), true)) {
                continue;
            }
            queryProfile.incScanFile(fragmentData.getSize());

            FragmentFileSearcher fragmentFileSearcher = new FragmentFileSearcher(fragment, fragmentData);
            fragmentFileSearcher.search(searchContext, collector);
        }

        collector.addCloseListener(new CloseListener() {
            @Override
            public void onClose() {
                for (DataSegmentFragment fragment : fragments) {
                    ColumnarStoreCache.getInstance().finishReadFragmentData(fragment);
                }
            }
        });
    }

    private FragmentData loadFragmentData(DataSegmentFragment fragment) throws IOException {
        if (queryProfile.isDetailProfileEnable()) {
            queryProfile.startStep(getLoadFragmentDataStep(fragment));
        }
        FragmentData fragmentData = ColumnarStoreCache.getInstance().startReadFragmentData(fragment);
        if (queryProfile.isDetailProfileEnable()) {
            queryProfile.finishStep(getLoadFragmentDataStep(fragment));
        }
        return fragmentData;
    }

    private String getLoadFragmentDataStep(DataSegmentFragment fragment) {
        return String.format(Locale.ROOT, "segment-%s_fragment-%s_load_data", segmentName,
                fragment.getFragmentId());
    }


}
