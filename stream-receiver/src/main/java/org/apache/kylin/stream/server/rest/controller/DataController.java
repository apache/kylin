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

package org.apache.kylin.stream.server.rest.controller;

import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.exception.StreamingException;
import org.apache.kylin.stream.core.model.DataRequest;
import org.apache.kylin.stream.core.model.DataResponse;
import org.apache.kylin.stream.core.query.IStreamingSearchResult;
import org.apache.kylin.stream.core.query.StreamingCubeDataSearcher;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.util.RecordsSerializer;
import org.apache.kylin.stream.server.StreamingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Controller
@RequestMapping(value = "/data")
public class DataController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(DataController.class);
    private StreamingServer streamingServer;

    public DataController() {
        streamingServer = StreamingServer.getInstance();
    }

    @RequestMapping(value = "/query", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public DataResponse query(@RequestBody DataRequest dataRequest) {
        IStreamingSearchResult searchResult = null;
        String queryId = dataRequest.getQueryId();
        StreamingQueryProfile queryProfile = new StreamingQueryProfile(queryId, dataRequest.getRequestSendTime());
        if (dataRequest.isEnableDetailProfile()) {
            queryProfile.enableDetailProfile();
        }
        if (dataRequest.getStorageBehavior() != null) {
            queryProfile.setStorageBehavior(StorageSideBehavior.valueOf(dataRequest.getStorageBehavior()));
        }
        StreamingQueryProfile.set(queryProfile);
        logger.info("receive query request queryId:{}", queryId);
        try {
            final Stopwatch sw = Stopwatch.createUnstarted();
            sw.start();
            String cubeName = dataRequest.getCubeName();
            long minSegmentTime = dataRequest.getMinSegmentTime();
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            CubeDesc cubeDesc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cubeName);

            Set<FunctionDesc> metrics = convertMetrics(cubeDesc, dataRequest.getMetrics());
            byte[] tupleFilterBytes = Base64.decodeBase64(dataRequest.getTupleFilter());
            TupleFilter tupleFilter = TupleFilterSerializer.deserialize(tupleFilterBytes, StringCodeSystem.INSTANCE);

            TupleFilter havingFilter = null;
            if (dataRequest.getHavingFilter() != null) {
                byte[] havingFilterBytes = Base64.decodeBase64(dataRequest.getHavingFilter());
                havingFilter = TupleFilterSerializer.deserialize(havingFilterBytes, StringCodeSystem.INSTANCE);
            }

            Set<TblColRef> dimensions = convertToTblColRef(dataRequest.getDimensions(), cubeDesc);
            Set<TblColRef> groups = convertToTblColRef(dataRequest.getGroups(), cubeDesc);

            StreamingSegmentManager segmentManager = streamingServer.getStreamingSegmentManager(cubeName);
            StreamingCubeDataSearcher dataSearcher = segmentManager.getSearcher();

            StreamingSearchContext gtSearchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups,
                    metrics, tupleFilter, havingFilter);
            searchResult = dataSearcher.doSearch(gtSearchRequest, minSegmentTime,
                    dataRequest.isAllowStorageAggregation());

            if (StorageSideBehavior.RAW_SCAN == queryProfile.getStorageBehavior()) {
                long counter = 0;
                for (Record record : searchResult) {
                    counter ++;
                }
                logger.info("query-{}: scan {} rows", queryId, counter);
            }
            RecordsSerializer serializer = new RecordsSerializer(gtSearchRequest.getRespResultSchema());
            Pair<byte[], Long> serializedRowsInfo = serializer.serialize(searchResult.iterator(),
                    dataRequest.getStoragePushDownLimit());
            DataResponse dataResponse = new DataResponse();
            dataResponse.setData(Base64.encodeBase64String(serializedRowsInfo.getFirst()));
            sw.stop();
            logger.info("query-{}: return response, took {} ms", queryId, sw.elapsed(MILLISECONDS));
            long finalCnt = serializedRowsInfo.getSecond();
            queryProfile.setFinalRows(finalCnt);
            String profileInfo = queryProfile.toString();
            dataResponse.setProfile(profileInfo);
            logger.info("query-{}: profile: {}", queryId, profileInfo);
            return dataResponse;
        } catch (Exception e) {
            throw new StreamingException(e);
        } finally {
            if (searchResult != null) {
                try {
                    searchResult.close();
                } catch (Exception e) {
                    logger.error("Fail to close result scanner, query id:" + queryId);
                }
            }
        }
    }

    private Set<FunctionDesc> convertMetrics(CubeDesc cubeDesc, List<FunctionDesc> metrics) {
        Set<FunctionDesc> result = Sets.newHashSet();
        for (FunctionDesc metric : metrics) {
            result.add(findAggrFuncFromCubeDesc(cubeDesc, metric));
        }
        return result;
    }

    private FunctionDesc findAggrFuncFromCubeDesc(CubeDesc cubeDesc, FunctionDesc aggrFunc) {
        aggrFunc.init(cubeDesc.getModel());
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        return aggrFunc;
    }

    private Set<TblColRef> convertToTblColRef(Set<String> columnSet, CubeDesc cubeDesc) {
        Set<TblColRef> result = Sets.newHashSet();
        for (String columnName : columnSet) {
            TblColRef tblColRef = cubeDesc.getModel().findColumn(columnName);
            result.add(tblColRef);
        }
        return result;
    }
}
