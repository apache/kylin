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

package io.kyligence.kap.newten.clickhouse;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.containers.startupcheck.StartupCheckStrategy;
import org.testcontainers.containers.traits.LinkableContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.utility.MountableFile;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Bind;

import lombok.NonNull;

public class VolumesContainer implements Container {

    private final com.github.dockerjava.api.model.Container innerContainer;

    public VolumesContainer(com.github.dockerjava.api.model.Container realContainer) {
        this.innerContainer = realContainer;
    }

    @Override
    public void setCommand(@NonNull String command) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCommand(@NonNull String... commandParts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addEnv(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode, SelinuxContext selinuxContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addLink(LinkableContainer otherContainer, String alias) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addExposedPort(Integer port) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addExposedPorts(int... ports) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container waitingFor(@NonNull WaitStrategy waitStrategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withFileSystemBind(String hostPath, String containerPath, BindMode mode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withVolumesFrom(Container container, BindMode mode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withExposedPorts(Integer... ports) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withCopyFileToContainer(MountableFile mountableFile, String containerPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withEnv(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withLabel(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withCommand(String cmd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withCommand(String... commandParts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withExtraHost(String hostname, String ipAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withNetworkMode(String networkMode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withNetwork(Network network) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withNetworkAliases(String... aliases) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withImagePullPolicy(ImagePullPolicy policy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withClasspathResourceMapping(String resourcePath, String containerPath, BindMode mode,
            SelinuxContext selinuxContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withStartupTimeout(Duration startupTimeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withPrivilegedMode(boolean mode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withMinimumRunningDuration(Duration minimumRunningDuration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withStartupCheckStrategy(StartupCheckStrategy strategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withWorkingDirectory(String workDir) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDockerImageName(@NonNull String dockerImageName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull String getDockerImageName() {
        return innerContainer.getImage();
    }

    @Override
    public String getTestHostIpAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Integer> getExposedPorts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getPortBindings() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InspectContainerResponse getContainerInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getExtraHosts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<String> getImage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getEnv() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getEnvMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getCommandParts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Bind> getBinds() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, LinkableContainer> getLinkedContainers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DockerClient getDockerClient() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCommandParts(String[] commandParts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWaitStrategy(WaitStrategy waitStrategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLinkedContainers(Map linkedContainers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinds(List list) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setEnv(List env) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setImage(Future image) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setExtraHosts(List extraHosts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPortBindings(List portBindings) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setExposedPorts(List exposedPorts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withLogConsumer(Consumer consumer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withLabels(Map labels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Container withEnv(Map env) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getContainerName() {
        return innerContainer.getNames()[0];
    }
}
