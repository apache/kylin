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

package org.apache.kylin.stream.core.storage;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.stream.core.exception.IllegalStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * checkpoint
 */
public class CheckPointStore {
    static final int CP_FILE_MAX_NUM = 5;
    private static final String CP_START = "====================";
    private static final String CP_END = "####################";
    private static final long DAY_TIMESTAMP_BASE = 24 * 3600 * 1000L;
    private static final String CP_FILE_PREFIX = "CP-";
    private static Logger logger = LoggerFactory.getLogger(CheckPointStore.class);
    private final File checkPointFolder;
    private final String cubeName;
    private final int maxNumOfCPFile;

    public CheckPointStore(String cubeName, File checkPointParent) {
        this(cubeName, checkPointParent, CP_FILE_MAX_NUM);
    }

    public CheckPointStore(String cubeName, File checkPointParent, int keepCPFileNum) {
        this.cubeName = cubeName;
        this.checkPointFolder = new File(checkPointParent, ".cp");
        if (checkPointFolder.exists() && !checkPointFolder.isDirectory()) {
            checkPointFolder.delete();
        }
        if (!checkPointFolder.exists()) {
            checkPointFolder.mkdirs();
        }
        this.maxNumOfCPFile = keepCPFileNum;
    }

    @VisibleForTesting
    File[] getCheckPointFiles() {
        File[] cpFiles = checkPointFolder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith(CP_FILE_PREFIX);
            }
        });
        if (cpFiles.length == 0) {
            return null;
        }
        return cpFiles;
    }

    private File getLatestCheckPointFile() {
        File[] cpFiles = getCheckPointFiles();
        if (cpFiles == null || cpFiles.length == 0) {
            return null;
        }

        File latestCheckPointFile = cpFiles[0];
        long latestTimestamp = getTimestampFromFileName(latestCheckPointFile.getName());
        for (int i = 1; i < cpFiles.length; i++) {
            long curTimestamp = getTimestampFromFileName(cpFiles[i].getName());
            if (curTimestamp > latestTimestamp) {
                latestTimestamp = curTimestamp;
                latestCheckPointFile = cpFiles[i];
            }
        }
        return latestCheckPointFile;
    }

    private File getCheckPointFile(CheckPoint cp) {
        File checkPointFile = new File(checkPointFolder, getFileNameFromTimestamp(cp.getCheckPointTime()));
        if (!checkPointFile.exists()) {
            try {
                checkPointFile.createNewFile();
                deleteOldCPFiles();
            } catch (IOException e) {
                throw new IllegalStorageException(e);
            }
        }
        return checkPointFile;
    }

    @VisibleForTesting
    void deleteOldCPFiles() {
        File[] cpFiles = getCheckPointFiles();
        if (cpFiles == null || cpFiles.length <= maxNumOfCPFile) {
            return;
        }

        ArrayList<File> cpFileList = Lists.newArrayList(cpFiles);
        Collections.sort(cpFileList, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        Iterator<File> fileIterator = cpFileList.subList(0, cpFileList.size() - maxNumOfCPFile).iterator();
        while (fileIterator.hasNext()) {
            File file = fileIterator.next();
            logger.info("going to delete checkpoint file " + file.getName());
            System.out.println("going to delete checkpoint file " + file.getName());
            file.delete();
        }
    }

    private String getFileNameFromTimestamp(long timestamp) {
        return CP_FILE_PREFIX + (timestamp / DAY_TIMESTAMP_BASE) * DAY_TIMESTAMP_BASE;
    }

    private long getTimestampFromFileName(String cpFileName) {
        return Long.valueOf(cpFileName.substring(CP_FILE_PREFIX.length()));
    }

    public void saveCheckPoint(CheckPoint cp) {
        try (FileOutputStream outputStream = FileUtils.openOutputStream(getCheckPointFile(cp), true)) {
            String jsonCP = JsonUtil.writeValueAsIndentString(cp);
            outputStream.write(Bytes.toBytes(wrapCheckPointString(jsonCP)));
            outputStream.flush();
        } catch (Exception e) {
            logger.error("CheckPoint error for cube " + cubeName, e);
        }
    }

    private String wrapCheckPointString(String checkpoint) {
        String lineSeparator = System.lineSeparator();
        return CP_START + lineSeparator + checkpoint + lineSeparator + CP_END + lineSeparator;
    }

    /**
     *
     * @return null if there is no valid checkpoint
     */
    public CheckPoint getLatestCheckPoint() {
        return getLatestCheckPoint(getLatestCheckPointFile());
    }

    private CheckPoint getLatestCheckPoint(File checkPointFile) {
        if (checkPointFile == null) {
            return null;
        }
        CheckPoint cp = null;
        try (ReversedLinesFileReader fileReader = new ReversedLinesFileReader(checkPointFile, 4096,
                Charset.forName("UTF-8"))) {
            String line = fileReader.readLine();
            while (!CP_END.equals(line) && line != null) {
                line = fileReader.readLine();
            }

            if (line == null) { //not found the checkpoint end line
                return null;
            }

            LinkedList<String> stack = Lists.newLinkedList();
            line = fileReader.readLine();
            while (!CP_START.equals(line) && line != null) {
                stack.push(line);
                line = fileReader.readLine();
            }
            if (line == null) {
                return null;
            }
            StringBuilder cpJson = new StringBuilder();
            while (!stack.isEmpty()) {
                cpJson.append(stack.pop());
            }
            cp = JsonUtil.readValue(cpJson.toString(), CheckPoint.class);
        } catch (IOException e) {
            logger.error("error when parse checkpoint");
        }
        return cp;
    }

    public void removeAllCheckPoints() {
        File[] cpFiles = getCheckPointFiles();
        if (cpFiles == null || cpFiles.length == 0) {
            return;
        }
        for (File cpFile : cpFiles) {
            removeCheckPoints(cpFile);
        }
    }

    private void removeCheckPoints(File checkPointFile) {
        try {
            FileUtils.write(checkPointFile, "");
        } catch (IOException e) {
            logger.error("error when remove all checkpoints");
        }
    }
}
