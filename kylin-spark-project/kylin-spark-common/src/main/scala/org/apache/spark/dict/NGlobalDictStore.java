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

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public abstract class NGlobalDictStore {

    // workingDir should be an absolute path, will create if not exists
    abstract void prepareForWrite(String workingDir) throws IOException;

    /**
     * @return all versions of this dictionary in ascending order
     * @throws IOException on I/O error
     */
    public abstract Long[] listAllVersions() throws IOException;

    // return the path of specified version dir
    public abstract Path getVersionDir(long version);

    public abstract NGlobalDictMetaInfo getMetaInfo(long version) throws IOException;

    public abstract Object2LongMap<String> getBucketDict(long version, NGlobalDictMetaInfo metadata, int bucketId)
            throws IOException;

    public abstract void writeBucketCurrDict(String workingPath, int bucketId, Object2LongMap<String> openHashMap)
            throws IOException;

    public abstract void writeBucketPrevDict(String workingPath, int bucketId, Object2LongMap<String> openHashMap)
            throws IOException;

    public abstract void writeMetaInfo(int bucketSize, String workingDir) throws IOException;

    public abstract void commit(String workingDir, int maxVersions, long versionTTL) throws IOException;
}
