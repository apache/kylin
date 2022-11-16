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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;

@Slf4j
public class AzureBlobClient {

    private final CloudBlobClient cloudBlobClient;
    private final BlobUrl blobUrl;

    public AzureBlobClient(CloudBlobClient cloudBlobClient, BlobUrl blobUrl) {
        this.cloudBlobClient = cloudBlobClient;
        this.blobUrl = blobUrl;
    }

    public static AzureBlobClient getInstance() {
        return Singletons.getInstance(AzureBlobClient.class, clientClass -> {
            String workDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
            BlobUrl blobUrl = BlobUrl.fromBlobUrl(workDir);
            URI endpoint = URI.create(blobUrl.getHttpEndpoint());
            StorageCredentials credentials = null;
            try {
                credentials = StorageCredentials.tryParseCredentials(blobUrl.getConnectionString());
            } catch (Exception e) {
                log.error("Blob connection string for working dir {} is error", workDir, e);
                ExceptionUtils.rethrow(e);
            }
            CloudBlobClient blobClient;
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                blobClient = CloudStorageAccount.getDevelopmentStorageAccount().createCloudBlobClient();
            } else {
                blobClient = new CloudBlobClient(endpoint, credentials);
            }
            return new AzureBlobClient(blobClient, blobUrl);
        });
    }

    public String generateSasKey(String blobPath, int expireHours) {
        try {
            CloudBlobContainer container = this.cloudBlobClient.getContainerReference(blobUrl.getContainer());
            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
            policy.setPermissionsFromString("r");
            policy.setSharedAccessStartTime(Date.from(OffsetDateTime.now(ZoneId.of("UTC")).minusHours(expireHours).toInstant()));
            policy.setSharedAccessExpiryTime(Date.from(OffsetDateTime.now(ZoneId.of("UTC")).plusHours(expireHours).toInstant()));
            return container.generateSharedAccessSignature(policy, null);
        } catch (URISyntaxException | StorageException | InvalidKeyException e) {
            log.error("generate SAS key for {} failed", blobPath, e);
            return ExceptionUtils.rethrow(e);
        }
    }

    public CloudBlob getBlob(CloudBlobContainer container, String path) {
        // blob name can't start with /
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        val containerClass = container.getClass();
        Method getBlobMethod = null;
        try {
            getBlobMethod = containerClass.getMethod("getBlobReferenceFromServer", String.class);
        } catch (NoSuchMethodException e) {
            // support hadoop-azure:3.2.0
        }
        if (getBlobMethod == null) {
            try {
                // support hadoop-azure:2.7.x
                getBlobMethod = containerClass.getMethod("getBlockBlobReference", String.class);
            } catch (NoSuchMethodException e) {
                log.error("Only support hadoop-azure 2.7.x, 3.2.x and 3.3.x, please check hadoop-azure version!", e);
                ExceptionUtils.rethrow(e);
            }
        }
        try {
            return (CloudBlob) getBlobMethod.invoke(container, path);
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.error("Can't access method {}", getBlobMethod.getName(), e);
            return ExceptionUtils.rethrow(e);
        }
    }


}
