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
package org.apache.kylin.rest;

import static org.apache.kylin.common.exception.ServerErrorCode.SYSTEM_IS_RECOVER;

import java.util.Objects;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import io.kyligence.kap.metadata.epoch.EpochManager;
import org.apache.kylin.rest.interceptor.ProjectInfoParser;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultResponse;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.loadbalancer.core.ReactorServiceInstanceLoadBalancer;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import lombok.val;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
public class ProjectBasedLoadBalancer implements ReactorServiceInstanceLoadBalancer {

    @Override
    // see original
    // https://github.com/Netflix/ocelli/blob/master/ocelli-core/
    // src/main/java/netflix/ocelli/loadbalancer/RoundRobinLoadBalancer.java
    public Mono<Response<ServiceInstance>> choose(Request request) {
        return Mono.fromCallable(this::getInstanceResponse);
    }

    private Response<ServiceInstance> getInstanceResponse() {
        HttpServletRequest httpServletRequest = ((ServletRequestAttributes) Objects
                .requireNonNull(RequestContextHolder.getRequestAttributes())).getRequest();
        Pair<String, HttpServletRequest> projectInfo = ProjectInfoParser.parseProjectInfo(httpServletRequest);
        String project = projectInfo.getFirst();
        String owner = EpochManager.getInstance().getEpochOwner(project);
        if (StringUtils.isBlank(owner)) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(SYSTEM_IS_RECOVER, msg.getLeadersHandleOver());
        }
        String[] host = owner.split(":");
        val serviceInstance = new DefaultServiceInstance("all", "all", host[0], Integer.parseInt(host[1]), false);
        log.info("Request {} is redirecting to project's owner node {}.", httpServletRequest.getRequestURI(),
                serviceInstance);

        return new DefaultResponse(serviceInstance);
    }
}