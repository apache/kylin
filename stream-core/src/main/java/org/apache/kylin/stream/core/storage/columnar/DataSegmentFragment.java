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

package org.apache.kylin.stream.core.storage.columnar;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.stream.core.exception.IllegalStorageException;
import org.apache.kylin.stream.core.exception.StreamingException;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.apache.kylin.stream.core.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSegmentFragment implements Comparable<DataSegmentFragment> {
    private static Logger logger = LoggerFactory.getLogger(DataSegmentFragment.class);
    private String baseStorePath;
    private String cubeName;
    private String segmentName;
    private FragmentId fragmentId;

    public DataSegmentFragment(String baseStorePath, String cubeName, String segmentName, FragmentId fragmentId) {
        this.baseStorePath = baseStorePath;
        this.cubeName = cubeName;
        this.segmentName = segmentName;
        this.fragmentId = fragmentId;
        createIfNotExist();
    }

    public void createIfNotExist() {
        File fragmentFolder = getFragmentFolder();
        if (!fragmentFolder.exists()) {
            fragmentFolder.mkdirs();
        }
    }

    public FragmentId getFragmentId() {
        return fragmentId;
    }

    public boolean isMergedFragment() {
        return fragmentId.getEndId() != fragmentId.getStartId();
    }

    public void purge() {
        File fragmentFolder = getFragmentFolder();
        try {
            FileUtils.deleteDirectory(fragmentFolder);
        } catch (IOException e) {
            logger.error("error happens when purge fragment", e);
        }
    }

    public File getDataFile() {
        File parentFolder = getFragmentFolder();
        File dataFile = new File(parentFolder, fragmentId + Constants.DATA_FILE_SUFFIX);
        if (!dataFile.exists()) {
            try {
                dataFile.createNewFile();
            } catch (IOException e) {
                throw new IllegalStorageException(e);
            }
        }
        return dataFile;
    }

    public long getDataFileSize() {
        return getDataFile().length();
    }

    public File getFragmentFolder() {
        return new File(baseStorePath + File.separator + cubeName + File.separator + segmentName + File.separator
                + fragmentId.toString());
    }

    public File getMetaFile() {
        File parentFolder = getFragmentFolder();
        File metaFile = new File(parentFolder, fragmentId + Constants.META_FILE_SUFFIX);
        return metaFile;
    }

    public FragmentMetaInfo getMetaInfo() {
        File metaFile = getMetaFile();
        if (metaFile == null || !metaFile.exists()) {
            return null;
        }
        try {
            return JsonUtil.readValue(metaFile, FragmentMetaInfo.class);
        } catch (IOException e) {
            throw new StreamingException("error when parse meta file");
        }
    }

    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DataSegmentFragment that = (DataSegmentFragment) o;

        if (cubeName != null ? !cubeName.equals(that.cubeName) : that.cubeName != null)
            return false;
        if (segmentName != null ? !segmentName.equals(that.segmentName) : that.segmentName != null)
            return false;
        return fragmentId != null ? fragmentId.equals(that.fragmentId) : that.fragmentId == null;

    }

    @Override
    public int hashCode() {
        int result = cubeName != null ? cubeName.hashCode() : 0;
        result = 31 * result + (segmentName != null ? segmentName.hashCode() : 0);
        result = 31 * result + (fragmentId != null ? fragmentId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" + "cubeName='" + cubeName + '\'' + ", segmentName='" + segmentName + '\'' + ", fragmentId="
                + fragmentId + '}';
    }

    @Override
    public int compareTo(DataSegmentFragment o) {
        if (!cubeName.equals(o.cubeName)) {
            return cubeName.compareTo(o.cubeName);
        }
        if (!segmentName.equals(o.segmentName)) {
            return segmentName.compareTo(o.segmentName);
        }

        return fragmentId.compareTo(o.fragmentId);
    }

}
