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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.stereotype.Component;

@Component("streamingMgmtService")
public class StreamingService extends BasicService {

    @Autowired
    private AccessService accessService;

    @PostFilter(Constant.ACCESS_POST_FILTER_READ)
    public List<StreamingConfig> listAllStreamingConfigs(final String table) throws IOException {
        List<StreamingConfig> streamingConfigs = new ArrayList();
        if (StringUtils.isEmpty(table)) {
            streamingConfigs = getStreamingManager().listAllStreaming();
        } else {
            StreamingConfig config = getStreamingManager().getConfig(table);
            if (config != null) {
                streamingConfigs.add(config);
            }
        }

        return streamingConfigs;
    }

    public List<StreamingConfig> getStreamingConfigs(final String table, final Integer limit, final Integer offset) throws IOException {

        List<StreamingConfig> streamingConfigs;
        streamingConfigs = listAllStreamingConfigs(table);

        if (limit == null || offset == null) {
            return streamingConfigs;
        }

        if ((streamingConfigs.size() - offset) < limit) {
            return streamingConfigs.subList(offset, streamingConfigs.size());
        }

        return streamingConfigs.subList(offset, offset + limit);
    }

    public StreamingConfig createStreamingConfig(StreamingConfig config) throws IOException {
        if (getStreamingManager().getStreamingConfig(config.getName()) != null) {
            throw new InternalErrorException("The streamingConfig named " + config.getName() + " already exists");
        }
        StreamingConfig streamingConfig = getStreamingManager().saveStreamingConfig(config);
        return streamingConfig;
    }

    //    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#desc, 'ADMINISTRATION') or hasPermission(#desc, 'MANAGEMENT')")
    public StreamingConfig updateStreamingConfig(StreamingConfig config) throws IOException {
        return getStreamingManager().updateStreamingConfig(config);
    }

    //    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#desc, 'ADMINISTRATION') or hasPermission(#desc, 'MANAGEMENT')")
    public void dropStreamingConfig(StreamingConfig config) throws IOException {
        getStreamingManager().removeStreamingConfig(config);
    }

}
