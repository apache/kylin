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

package org.apache.kylin.dict.global;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;

import java.io.IOException;

public abstract class GlobalDictStore {

    protected final String baseDir; // base directory containing all versions of this global dict
    protected final int maxVersions;
    protected final int versionTTL;

    protected GlobalDictStore(String baseDir) {
        this.baseDir = Preconditions.checkNotNull(baseDir, "baseDir");
        this.maxVersions = KylinConfig.getInstanceFromEnv().getAppendDictMaxVersions();
        this.versionTTL = KylinConfig.getInstanceFromEnv().getAppendDictVersionTTL();
    }

    // workingDir should be an absolute path, will create if not exists
    abstract void prepareForWrite(String workingDir, boolean isGlobal) throws IOException;

    /**
     * @return all versions of this dictionary in ascending order
     * @throws IOException on I/O error
     */
    public abstract Long[] listAllVersions() throws IOException;

    // return the path of specified version dir
    public abstract Path getVersionDir(long version);

    /**
     * Get the metadata for a particular version of the dictionary.
     * @param version version number
     * @return <i>GlobalDictMetadata</i> for the specified version
     * @throws IOException on I/O error
     */
    public abstract GlobalDictMetadata getMetadata(long version) throws IOException;

    /**
     * Read a <i>DictSlice</i> from a slice file.
     * @param workingDir directory of the slice file
     * @param sliceFileName file name of the slice
     * @return a <i>DictSlice</i>
     * @throws IOException on I/O error
     */
    public abstract AppendDictSlice readSlice(String workingDir, String sliceFileName) throws IOException;

    /**
     * Write a slice with the given key to the specified directory.
     * @param workingDir where to write the slice, should exist
     * @param key slice key
     * @param slice slice to write
     * @return file name of the new written slice
     * @throws IOException on I/O error
     */
    public abstract String writeSlice(String workingDir, AppendDictSliceKey key, AppendDictNode slice) throws IOException;

    /**
     * Delete a slice with the specified file name.
     * @param workingDir directory of the slice file, should exist
     * @param sliceFileName file name of the slice, should exist
     * @throws IOException on I/O error
     */
    public abstract void deleteSlice(String workingDir, String sliceFileName) throws IOException;

    /**
     * commit the <i>DictSlice</i> and <i>GlobalDictMetadata</i> in workingDir to new versionDir
     * @param workingDir where store the tmp slice and index, should exist
     * @param globalDictMetadata the metadata of global dict
     * @param isAppendDictGlobal mark the append dict whether is global or not
     * @throws IOException on I/O error
     */
    public abstract void commit(String workingDir, GlobalDictMetadata globalDictMetadata, boolean isAppendDictGlobal) throws IOException;

    /**
     * Copy the latest version of this dict to another meta. The source is unchanged.
     * @param srcConfig config of source meta
     * @param dstConfig config of destination meta
     * @return the new base directory for destination meta
     * @throws IOException on I/O error
     */
    public abstract String copyToAnotherMeta(KylinConfig srcConfig, KylinConfig dstConfig) throws IOException;
}
