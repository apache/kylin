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

package org.apache.kylin.metadata;

/**
 * Constances to describe metadata and it's change.
 *
 */
public interface MetadataConstants {

    public static final String FILE_SURFIX = ".json";
    public static final String PROJECT_RESOURCE = "project";
    public static final String TYPE_VERSION = ".version";

    //Identifier Type, user or group
    public static final String TYPE_USER = "user";
    public static final String TYPE_GROUP = "group";

    // Extended attribute keys
    public static final String TABLE_EXD_STATUS_KEY = "EXD_STATUS";
    public static final String TABLE_EXD_MINFS = "minFileSize";
    public static final String TABLE_EXD_TNF = "totalNumberFiles";
    public static final String TABLE_EXD_LOCATION = "location";
    public static final String TABLE_EXD_LUT = "lastUpdateTime";
    public static final String TABLE_EXD_LAT = "lastAccessTime";
    public static final String TABLE_EXD_COLUMN = "columns";
    public static final String TABLE_EXD_PC = "partitionColumns";
    public static final String TABLE_EXD_MAXFS = "maxFileSize";
    public static final String TABLE_EXD_IF = "inputformat";
    public static final String TABLE_EXD_PARTITIONED = "partitioned";
    public static final String TABLE_EXD_TABLENAME = "tableName";
    public static final String TABLE_EXD_OWNER = "owner";
    public static final String TABLE_EXD_TFS = "totalFileSize";
    public static final String TABLE_EXD_OF = "outputformat";
    /**
     * The value is an array
     */
    public static final String TABLE_EXD_CARDINALITY = "cardinality";
    public static final String TABLE_EXD_DELIM = "delim";
    public static final String TABLE_EXD_DEFAULT_VALUE = "unknown";

}
