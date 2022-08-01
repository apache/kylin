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

import com.google.common.base.Preconditions;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;

import java.io.File;
import java.net.URI;
import java.util.Locale;

public enum TableSourceType {
    HDFS {
        @Override
        public TableEngineType getTableEngineType() {
            return TableEngineType.HDFS;
        }

        @Override
        public String transformFileUrl(String file, String sitePath, URI rootPath) {
            Preconditions.checkArgument(file.startsWith("hdfs") || file.startsWith("viewfs"),
                    "file %s should start with hdfs or viewfs", file);
            if (file.startsWith("viewfs")){
                file = ViewFsTransform.getInstance().generateFileUrl(file);
            }
            return String.format(Locale.ROOT, "HDFS('%s' , Parquet)", file);
        }
    },
    // the datasource type is for unit test
    UT {
        @Override
        public TableEngineType getTableEngineType() {
            return TableEngineType.URL;
        }

        @Override
        public String transformFileUrl(String file, String sitePath, URI rootPath) {
            final String localSchema = "file:";
            Preconditions.checkArgument(file.startsWith(localSchema));
            URI thisPathURI = new File(file.substring(localSchema.length())).toURI();
            return String.format(Locale.ROOT, "URL('%s/%s' , Parquet)", sitePath, rootPath.relativize(thisPathURI).getPath());
        }
    },
    BLOB {
        @Override
        public TableEngineType getTableEngineType() {
            return TableEngineType.URL;
        }

        @Override
        public String transformFileUrl(String file, String sitePath, URI rootPath) {
            // wasb://test@devstoreaccount1.localhost:10002/kylin
            URI blobUri = URI.create(file);
            String schema;
            switch (blobUri.getScheme()) {
                case "wasb":
                    schema = "http";
                    break;
                case "wasbs":
                    schema = "https";
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format(Locale.ROOT, "Unspported schema %s", blobUri.getScheme()));
            }
            String accountName = blobUri.getHost().split("\\.")[0];
            String container = blobUri.getUserInfo();
            String host = blobUri.getHost();
            int port = blobUri.getPort();
            String resourcePath = blobUri.getPath();
            String url;
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                // sitePath =  host.docker.internal
                url = String.format(Locale.ROOT, "%s://%s:%d/%s/%s%s", schema, sitePath, port, accountName, container, resourcePath);
            } else {
                url = String.format(Locale.ROOT, "%s://%s/%s%s", schema, host, container, resourcePath);
            }
            AzureBlobClient client = AzureBlobClient.getInstance();
            String sasKey = client.generateSasKey(file, 1);
            return String.format(Locale.ROOT, "URL('%s?%s' , Parquet)", url, sasKey);
        }
    },
    S3 {
        @Override
        public TableEngineType getTableEngineType() {
            return TableEngineType.S3;
        }

        @Override
        public String transformFileUrl(String file, String sitePath, URI rootPath) {
//             s3a://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet
            URI uri = URI.create(file);
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                // sitepath = host.docker.internal:9000&test&test123
                String[] urlParts = sitePath.split("&");
                String url = urlParts[0];
                String accessKey = urlParts[1];
                String secretKey = urlParts[2];
                String path = String.format(Locale.ROOT, "http://%s/%s%s", url, uri.getHost(), uri.getPath());
                return String.format(Locale.ROOT, "S3('%s' , '%s','%s', Parquet)", path, accessKey, secretKey);
            } else {
                String region = HadoopUtil.getCurrentConfiguration().get("fs.s3a.region");
                boolean isCn = region.startsWith("cn");
                String endpoint;
                if (isCn) {
                    endpoint = "amazonaws.com.cn";
                } else {
                    endpoint = "amazonaws.com";
                }
                String path = String.format(Locale.ROOT, "https://%s.s3.%s.%s%s", uri.getHost(), region,
                        endpoint, uri.getPath());
                return String.format(Locale.ROOT, "S3('%s' , Parquet)", path);
            }
        }
    };

    public abstract TableEngineType getTableEngineType();

    public abstract String transformFileUrl(String file, String sitePath, URI rootPath);
}
