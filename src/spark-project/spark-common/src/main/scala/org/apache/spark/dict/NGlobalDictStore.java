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

package org.apache.spark.dict;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import it.unimi.dsi.fastutil.objects.Object2LongMap;

public interface NGlobalDictStore {

    // workingDir should be an absolute path, will create if not exists
    void prepareForWrite(String workingDir) throws IOException;

    /**
     * @return all versions of this dictionary in ascending order
     * @throws IOException on I/O error
     */
    Long[] listAllVersions() throws IOException;

    // return the path of specified version dir
    Path getVersionDir(long version);

    NGlobalDictMetaInfo getMetaInfo(long version) throws IOException;

    Object2LongMap<String> getBucketDict(long version, NGlobalDictMetaInfo metadata, int bucketId) throws IOException;

    void writeBucketCurrDict(String workingPath, int bucketId, Object2LongMap<String> openHashMap) throws IOException;

    void writeBucketPrevDict(String workingPath, int bucketId, Object2LongMap<String> openHashMap) throws IOException;

    void writeMetaInfo(int bucketSize, String workingDir) throws IOException;

    void commit(String workingDir, int maxVersions, long versionTTL) throws IOException;

    String getWorkingDir();
}
