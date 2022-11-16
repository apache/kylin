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

import org.testcontainers.containers.FixedHostPortGenericContainer;

public class AzuriteContainer extends FixedHostPortGenericContainer<AzuriteContainer> {
    public static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.9.0";
    public static final String COMMAND = "azurite --skipApiVersionCheck --blobHost 0.0.0.0 -l /data";
    public static final int DEFAULT_BLOB_PORT = 10000;

    private int port;
    private String path;

    public AzuriteContainer(int blobPort, String path) {
        super(AZURITE_IMAGE);
        this.port = blobPort;
        this.path = path;
    }

    @Override
    public void start() {
        this.withFixedExposedPort(port, DEFAULT_BLOB_PORT);
        this.withCommand(COMMAND);
        this.withFileSystemBind(path, "/data");
        super.start();
    }
}
