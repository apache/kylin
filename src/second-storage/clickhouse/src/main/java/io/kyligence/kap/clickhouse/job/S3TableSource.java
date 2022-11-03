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

import com.amazonaws.util.EC2MetadataUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3TableSource implements AbstractTableSource {
    private final String region;
    private final String endpoint;

    public S3TableSource() {
        this.region = EC2MetadataUtils.getEC2InstanceRegion();
        log.info("EC2 metadata region is {}", this.region);
        if (this.region.startsWith("cn")) {
            endpoint = "amazonaws.com.cn";
        } else {
            endpoint = "amazonaws.com";
        }
    }

    @Override
    public String transformFileUrl(String file, String sitePath, URI rootPath) {
        // s3a://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet
        URI uri = URI.create(file);
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            // sitepath = host.docker.internal:9000&test&test123
            String[] urlParts = sitePath.split("&");
            String url = urlParts[0];
            String accessKey = urlParts[1];
            String secretKey = urlParts[2];
            return String.format(Locale.ROOT, "S3('http://%s/%s%s', '%s','%s', Parquet)", url, uri.getHost(),
                    uri.getPath(), accessKey, secretKey);
        } else {
            return String.format(Locale.ROOT, "S3('https://%s.s3.%s.%s%s', Parquet)", uri.getHost(), region, endpoint,
                    uri.getPath());
        }
    }
}
