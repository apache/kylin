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
package io.kyligence.kap.cache.fs;

public class CacheFileSystemConstants {

    private CacheFileSystemConstants() {
    }

    public static final String PARAMS_KEY_USE_CACHE =
            "spark.kylin.local-cache.enabled";

    public static final boolean PARAMS_KEY_USE_CACHE_DEFAULT_VALUE = false;

    public static final String PARAMS_KEY_USE_FILE_STATUS_CACHE =
            "spark.kylin.file-status-cache.enabled";

    public static final boolean PARAMS_KEY_USE_FILE_STATUS_CACHE_DEFAULT_VALUE = false;

    public static final String PARAMS_KEY_IO_FILE_BUFFER_SIZE = "io.file.buffer.size";

    public static final int PARAMS_KEY_IO_FILE_BUFFER_SIZE_DEFAULT_VALUE = 65536;

    public static final String PARAMS_KEY_FILE_STATUS_CACHE_TTL =
            "spark.kylin.local-cache.filestatus-cache-ttl";

    public static final long PARAMS_KEY_FILE_STATUS_CACHE_TTL_DEFAULT_VALUE = 3600L;

    public static final String PARAMS_KEY_FILE_STATUS_CACHE_MAX_SIZE =
            "spark.kylin.local-cache.filestatus-cache-max-size";

    public static final long PARAMS_KEY_FILE_STATUS_CACHE_MAX_SIZE_DEFAULT_VALUE = 10000L;

    public static final String PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM =
            "spark.kylin.local-cache.use.legacy.file-input-stream";

    public static final boolean PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM_DEFAULT_VALUE = false;

    public static final String PARAMS_KEY_USE_BUFFER_FILE_INPUTSTREAM =
            "spark.kylin.local-cache.use.buffer.file-input-stream";

    public static final boolean PARAMS_KEY_USE_BUFFER_FILE_INPUTSTREAM_DEFAULT_VALUE = false;

    public static final String PARAMS_KEY_LOCAL_CACHE_FOR_CURRENT_FILES =
            "spark.kylin.local-cache.for.current.files";
}
