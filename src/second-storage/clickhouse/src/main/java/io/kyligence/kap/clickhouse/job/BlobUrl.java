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

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.util.HadoopUtil;

import java.net.URI;
import java.util.Locale;

@Data
public class BlobUrl {
    private String accountName;
    private String accountKey;
    private String container;
    private String hostSuffix;
    private int port;
    private String path;
    private String httpSchema;
    private String blobSchema;

    public static BlobUrl fromHttpUrl(String url) {
        URI uri = URI.create(url);
        BlobUrl blobUrl = new BlobUrl();
        blobUrl.setHttpSchema(uri.getScheme());
        blobUrl.setBlobSchema(http2blobSchema(uri.getScheme()));
        blobUrl.setAccountName(uri.getHost().split("\\.")[0]);
        blobUrl.setContainer(uri.getPath().split("/")[1]);
        blobUrl.setHostSuffix(uri.getHost().replace(blobUrl.getAccountName() + ".", ""));
        blobUrl.setPort(uri.getPort());
        blobUrl.setPath(uri.getPath().replace("/" + blobUrl.getContainer(), ""));
        blobUrl.setAccountKey(getAccountKeyByName(blobUrl.getAccountName(),
                blobUrl.getHostSuffix(), blobUrl.getPort()));
        return blobUrl;
    }

    public static BlobUrl fromBlobUrl(String url) {
        URI uri = URI.create(url);
        BlobUrl blobUrl = new BlobUrl();
        blobUrl.setHttpSchema(blob2httpSchema(uri.getScheme()));
        blobUrl.setBlobSchema(uri.getScheme());
        blobUrl.setAccountName(uri.getHost().split("\\.")[0]);
        blobUrl.setContainer(uri.getUserInfo());
        blobUrl.setHostSuffix(uri.getHost().replace(blobUrl.getAccountName() + ".", ""));
        blobUrl.setPort(uri.getPort());
        blobUrl.setPath(uri.getPath());
        blobUrl.setAccountKey(getAccountKeyByName(blobUrl.getAccountName(),
                blobUrl.getHostSuffix(), blobUrl.getPort()));
        return blobUrl;
    }

    private static String getAccountKeyByName(String accountName, String host, int port) {
        Configuration configuration = HadoopUtil.getCurrentConfiguration();
        String configKey;
        if (port == -1) {
            configKey = String.format(Locale.ROOT, "%s.%s", accountName, host);
        } else {
            configKey = String.format(Locale.ROOT, "%s.%s:%s", accountName, host, port);
        }
        return configuration.get(String.format(Locale.ROOT, "fs.azure.account.key.%s", configKey));
    }

    private static String http2blobSchema(String httpSchema) {
        String schema;
        switch (httpSchema) {
            case "http":
                schema = "wasb";
                break;
            case "https":
                schema = "wasbs";
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(Locale.ROOT, "Unsupported schema %s", httpSchema));
        }
        return schema;
    }

    public static String blob2httpSchema(String blobSchema) {
        String schema;
        switch (blobSchema) {
            case "wasb":
                schema = "http";
                break;
            case "abfs":
            case "abfss":
            case "wasbs":
                schema = "https";
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(Locale.ROOT, "Unsupported schema %s", blobSchema));
        }
        return schema;
    }

    public String getHttpEndpoint() {
        String defaultPort = this.getPort() == -1 ? "" : ":" + this.getPort();
        return String.format(Locale.ROOT, "%s://%s.%s%s", this.getHttpSchema(), this.getAccountName(),
                this.getHostSuffix(), defaultPort);
    }

    public String getConnectionString() {
        String formatHostSuffix = this.getHostSuffix().replace("blob.", "");
        return String.format(Locale.ROOT, "DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;EndpointSuffix=%s",
                this.getHttpSchema(), this.getAccountName(), this.getAccountKey(), formatHostSuffix);
    }
}
