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

package io.kyligence.kap.clickhouse.management;

import com.google.common.base.Preconditions;
import io.kyligence.kap.secondstorage.SecondStorageConfigLoader;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.ClickHouseConfig;
import org.apache.kylin.common.Singletons;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class ClickHouseConfigLoader implements SecondStorageConfigLoader {

    private final File configFile;
    private final AtomicReference<ClusterInfo> cluster = new AtomicReference<>();

    private ClickHouseConfigLoader(File configFile) {
        this.configFile = configFile;
    }

    public static ClickHouseConfigLoader getInstance() {
        return Singletons.getInstance(ClickHouseConfigLoader.class, clazz -> {
            File configFile = new File(ClickHouseConfig.getInstanceFromEnv().getClusterConfig());
            ClickHouseConfigLoader clickHouseConfigLoader = new ClickHouseConfigLoader(configFile);
            clickHouseConfigLoader.load();
            return clickHouseConfigLoader;
        });
    }

    public static void clean() {
        Singletons.clearInstance(ClickHouseConfigLoader.class);
    }

    public static Yaml getConfigYaml() {
        Constructor constructor = new Constructor(ClusterInfo.class);
        val clusterDesc = new TypeDescription(ClusterInfo.class);
        clusterDesc.addPropertyParameters("cluster", String.class, List.class);
        clusterDesc.addPropertyParameters("socketTimeout", String.class);
        clusterDesc.addPropertyParameters("keepAliveTimeout", String.class);
        clusterDesc.addPropertyParameters("installPath", String.class);
        clusterDesc.addPropertyParameters("logPath", String.class);
        clusterDesc.addPropertyParameters("userName", String.class);
        clusterDesc.addPropertyParameters("password", String.class);
        constructor.addTypeDescription(clusterDesc);
        val nodeDesc = new TypeDescription(Node.class);
        nodeDesc.addPropertyParameters("name", String.class);
        nodeDesc.addPropertyParameters("ip", String.class);
        nodeDesc.addPropertyParameters("port", Integer.class);
        nodeDesc.addPropertyParameters("user", String.class);
        nodeDesc.addPropertyParameters("password", String.class);
        nodeDesc.addPropertyParameters("sshPort", String.class);
        constructor.addTypeDescription(nodeDesc);
        return new Yaml(constructor);
    }

    @Override
    public void load() {
        Yaml yaml = getConfigYaml();
        try {
            ClusterInfo config = yaml.load(new FileInputStream(configFile));
            config.transformNode();
            val pairSizeList = config.getCluster().values().stream().map(List::size).collect(Collectors.toSet());
            Preconditions.checkState(pairSizeList.size() <= 1, "There are different size node pair.");
            val allNodes = config.getNodes();
            Preconditions.checkState(allNodes.size() == allNodes.stream().map(Node::getName).collect(Collectors.toSet()).size(),
                    "There are duplicate node name");
            cluster.set(config);
        } catch (FileNotFoundException e) {
            log.error("ClickHouse config file {} not found", configFile.getAbsolutePath());
        }
    }

    public static class ClusterConstructor extends Constructor {

    }

    @Override
    public void refresh() {
        clean();
        getInstance();
    }

    @Override
    public ClusterInfo getCluster() {
        return new ClusterInfo(cluster.get());
    }
}
