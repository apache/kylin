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

package io.kyligence.kap.clickhouse.job;

import java.net.URI;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;

public class BlobTableSource implements AbstractTableSource {
    @Override
    public String transformFileUrl(String file, String sitePath, URI rootPath) {
        // wasb://test@devstoreaccount1.localhost:10002/kylin
        URI blobUri = URI.create(file);
        String schema = BlobUrl.blob2httpSchema(blobUri.getScheme());
        String accountName = blobUri.getHost().split("\\.")[0];
        String container = blobUri.getUserInfo();
        String host = blobUri.getHost();
        int port = blobUri.getPort();
        String resourcePath = blobUri.getPath();
        String sasKey = AzureBlobClient.getInstance().generateSasKey(file, 1);
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            // sitePath =  host.docker.internal
            return String.format(Locale.ROOT, "URL('%s://%s:%d/%s/%s%s?%s' , Parquet)", schema, sitePath, port,
                    accountName, container, resourcePath, sasKey);
        } else {
            return String.format(Locale.ROOT, "URL('%s://%s/%s%s?%s' , Parquet)", schema, host, container, resourcePath,
                    sasKey);
        }
    }
}
