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

package org.apache.kylin.storage.druid.http;

import org.joda.time.Duration;

public class HttpClientConfig {
    public static final CompressionCodec DEFAULT_COMPRESSION_CODEC = CompressionCodec.GZIP;
    // Default from NioClientSocketChannelFactory.DEFAULT_BOSS_COUNT, which is private:
    private static final int DEFAULT_BOSS_COUNT = 1;
    // Default from SelectorUtil.DEFAULT_IO_THREADS, which is private:
    private static final int DEFAULT_WORKER_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private final int numConnections;
    private final Duration readTimeout;
    private final int bossPoolSize;
    private final int workerPoolSize;
    private final CompressionCodec compressionCodec;

    private HttpClientConfig(
            int numConnections,
            Duration readTimeout,
            int bossPoolSize,
            int workerPoolSize,
            CompressionCodec compressionCodec
    ) {
        this.numConnections = numConnections;
        this.readTimeout = readTimeout;
        this.bossPoolSize = bossPoolSize;
        this.workerPoolSize = workerPoolSize;
        this.compressionCodec = compressionCodec;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getNumConnections() {
        return numConnections;
    }

    public Duration getReadTimeout() {
        return readTimeout;
    }

    public int getBossPoolSize() {
        return bossPoolSize;
    }

    public int getWorkerPoolSize() {
        return workerPoolSize;
    }

    public CompressionCodec getCompressionCodec() {
        return compressionCodec;
    }

    public enum CompressionCodec {
        IDENTITY {
            @Override
            public String getEncodingString() {
                return "identity";
            }
        },
        GZIP {
            @Override
            public String getEncodingString() {
                return "gzip";
            }
        },
        DEFLATE {
            @Override
            public String getEncodingString() {
                return "deflate";
            }
        };

        /**
         * Get the header-ified name of this encoding, which should go in "Accept-Encoding" and
         * "Content-Encoding" headers. This is not just the lowercasing of the enum name, since
         * we may one day support x- encodings like LZ4, which would likely be an enum named
         * "LZ4" that has an encoding string like "x-lz4".
         *
         * @return encoding name
         */
        public abstract String getEncodingString();
    }

    public static class Builder {
        private int numConnections = 1;
        private Duration readTimeout = null;
        private int bossCount = DEFAULT_BOSS_COUNT;
        private int workerCount = DEFAULT_WORKER_COUNT;
        private CompressionCodec compressionCodec = DEFAULT_COMPRESSION_CODEC;

        private Builder() {
        }

        public Builder withNumConnections(int numConnections) {
            this.numConnections = numConnections;
            return this;
        }

        public Builder withReadTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder withBossCount(int bossCount) {
            this.bossCount = bossCount;
            return this;
        }

        public Builder withWorkerCount(int workerCount) {
            this.workerCount = workerCount;
            return this;
        }

        public Builder withCompressionCodec(CompressionCodec compressionCodec) {
            this.compressionCodec = compressionCodec;
            return this;
        }

        public HttpClientConfig build() {
            return new HttpClientConfig(
                    numConnections,
                    readTimeout,
                    bossCount,
                    workerCount,
                    compressionCodec
            );
        }
    }
}
