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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.interceptor.ProjectInfoParser;
import org.apache.kylin.rest.response.ServerInfoResponse;
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
public class ResourceGroupLoadBalancer implements ReactorServiceInstanceLoadBalancer {

    private ClusterManager clusterManager;

    public ResourceGroupLoadBalancer(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

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
        ResourceGroupManager rgManager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<String> jobNodes = clusterManager.getJobServers().stream().map(ServerInfoResponse::getHost)
                .collect(Collectors.toList());
        String project = ProjectInfoParser.parseProjectInfo(httpServletRequest).getFirst();
        if (rgManager.isResourceGroupEnabled() && !project.equals(UnitOfWork.GLOBAL_UNIT)) {
            jobNodes.retainAll(rgManager.getInstancesForProject(project));
        }

        if (jobNodes.isEmpty()) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(SYSTEM_IS_RECOVER, msg.getLeadersHandleOver());
        }
        String randomNode = jobNodes.get(RandomUtil.nextInt(jobNodes.size()));
        String[] host = randomNode.split(":");
        val serviceInstance = new DefaultServiceInstance("all", "all", host[0], Integer.parseInt(host[1]), false);
        log.info("Request {} is redirecting to random job node {}.", httpServletRequest.getRequestURI(),
                serviceInstance);

        return new DefaultResponse(serviceInstance);
    }
}
