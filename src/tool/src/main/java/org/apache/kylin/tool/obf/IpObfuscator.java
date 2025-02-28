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
package org.apache.kylin.tool.obf;

import static org.apache.kylin.tool.constant.SensitiveConfigKeysConstant.HIDDEN;
import static org.apache.kylin.tool.util.ToolUtil.existsLinuxCommon;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.val;

public class IpObfuscator extends FileObfuscator {

    private List<String> zipFiles = new ArrayList<>();

    private Map<String, String> ipDirHiddens = Maps.newHashMap();

    private static final String IP_NETWORK_NUMBER = "((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){3}";
    private static final String IP_HOST_NUMBER = "(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]\\d|[1-9])";

    private static final String IP_PATTERN = IP_NETWORK_NUMBER + IP_HOST_NUMBER;

    private static final int BUFFER_SIZE = 10 * 1024 * 1024;

    private static final Pattern IP_PATTERN_PATTERN = Pattern.compile("(.*)" + IP_PATTERN + "(.*)");

    public IpObfuscator(ObfLevel level, MappingRecorder recorder, ResultRecorder resultRecorder) {
        super(level, recorder, resultRecorder);
    }

    @Override
    public void doObfuscateFile(File orig) {
        String path = orig.getAbsolutePath();
        File tmpFile = new File(orig.getAbsolutePath() + ".tmp");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(orig), Charset.defaultCharset()), BUFFER_SIZE);
                BufferedWriter bw = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(tmpFile), Charset.defaultCharset()), BUFFER_SIZE)) {
            String readLine;
            while ((readLine = reader.readLine()) != null) {
                bw.write(readLine.replaceAll(IP_PATTERN, HIDDEN));
                bw.newLine();
            }
            bw.flush();
            boolean renameResult = tmpFile.renameTo(orig);
            if (!renameResult) {
                logger.error("{} processed failed", path);
                resultRecorder.addFailedFile(path);
            }
            logger.info("{} processed successfully", path);
            resultRecorder.addSuccessFile(path);
        } catch (IOException e) {
            logger.error("{} processed failed", path, e);
            resultRecorder.addFailedFile(path);
        }
    }

    @Override
    public void obfuscate(File orig, FileFilter fileFilter) {
        ipObfuscatorPrepare(orig);
        if (fileFilter == null && existsLinuxCommon("sed")) {
            obfuscateByLinux(orig);
        } else {
            super.obfuscate(orig, fileFilter);
        }
        ipObfuscatorClean();
    }

    public void obfuscateByLinux(File rootDir) {
        logger.info("diag start obf ip by linux");
        String rex = "([0-9]{1,3}\\.){3}(([0-9]{2,3})|[1-9])";
        String finalCommand = String.format(Locale.ROOT,
                "cd %s/ && sed -ri 's/%s/<hidden>/g' $(grep -E '%s' -rl %s)", KylinConfig.getKylinHome(), rex, rex,
                rootDir.getAbsolutePath());
        CliCommandExecutor commandExecutor = new CliCommandExecutor();
        val patternedLogger = new BufferedLogger(logger);
        String uuid = rootDir.getAbsolutePath();

        try {
            logger.info("command = {}", finalCommand);
            commandExecutor.execute(finalCommand, patternedLogger, uuid);
            logger.info("diag end obf ip by linux");
        } catch (ShellException e) {
            logger.error("Failed to execute obf ip diag by linux", e);
            super.obfuscate(rootDir, null);
        }
    }

    public static boolean existsIp(String text) {
        Matcher ipMatcher = IP_PATTERN_PATTERN.matcher(text);
        return ipMatcher.matches();
    }

    public void ipObfuscatorPrepare(File root) {
        logger.info("ip obfuscator do prepare");
        this.zipFiles = findZipFileAndUnzip(root);
        this.ipDirHiddens = findIpDir(root);
        changeZipFileByIpDirs(root);
        moveFileByIpDirs();
    }

    public void moveFileByIpDirs() {
        logger.info("ip obfuscator do move file by ip dirs");
        for (Map.Entry<String, String> entry : this.ipDirHiddens.entrySet()) {
            File srcFile = new File(entry.getKey());
            File destFile = new File(entry.getValue());

            destFile.getParentFile().mkdirs();
            try {
                FileUtils.moveFile(srcFile, destFile);
                removeEmptyParentsDirectory(srcFile.getParentFile());
            } catch (IOException e) {
                logger.error("{} processed move file By ipDirs failed", entry.getKey(), e);
                resultRecorder.addFailedFile(entry.getKey());
            }
        }
    }

    /**
     * remove all empty parents directory
     * @param parentFile
     */
    public static void removeEmptyParentsDirectory(File parentFile) throws IOException {
        if (Array.isEmpty(parentFile.listFiles())) {
            Files.delete(parentFile.toPath());
            removeEmptyParentsDirectory(parentFile.getParentFile());
        }

    }

    private void changeZipFileByIpDirs(File root) {
        logger.info("ip obfuscator do change zip file name by ip dirs");
        List<String> ipDirZips = zipFiles.stream().filter(file -> ipDirHiddens.containsKey(file))
                .collect(Collectors.toList());
        if (ipDirZips.isEmpty()) {
            return;
        }
        this.zipFiles.removeAll(ipDirZips);
        List<String> hiddenIpZipFiles = ipDirZips.stream().map(ipDirZip -> getHiddenIpDir(root, ipDirZip))
                .collect(Collectors.toList());
        this.zipFiles.addAll(hiddenIpZipFiles);
    }

    private Map<String, String> findIpDir(File root) {
        List<String> ipDirs = findFiles(root, file -> existsIp(file.getAbsolutePath()));
        Map<String, String> ipDirHidden = Maps.newHashMap();
        if (ipDirs.isEmpty()) {
            return ipDirHidden;
        }

        for (String ipDir : ipDirs) {
            String hiddenIpDir = getHiddenIpDir(root, ipDir);
            ipDirHidden.put(ipDir, hiddenIpDir);
        }
        return ipDirHidden;
    }

    public static String getHiddenIpDir(File root, String ipDir) {
        return root.getAbsolutePath() + ipDir.replace(root.getAbsolutePath(), "").replaceAll(IP_PATTERN, HIDDEN);
    }

    public List<String> findZipFileAndUnzip(File root) {
        List<String> zipFileList = findFiles(root, file -> file.getName().endsWith(".zip"));

        for (String zipFile : zipFileList) {
            try {
                ZipFileUtils.decompressZipFile(zipFile, new File(zipFile).getParent());
            } catch (IOException e) {
                logger.error("{} processed decompress zip failed", zipFile, e);
                resultRecorder.addFailedFile(zipFile);
            }
        }
        return zipFileList;
    }

    public void ipObfuscatorClean() {
        logger.info("ip obfuscator do clean");
        compressZipFileAndCleanZipDir();
    }

    private void compressZipFileAndCleanZipDir() {
        logger.info("ip obfuscator do compress zip file and clean zip dir");
        for (String zipFile : zipFiles) {
            try {
                String zipDir = zipFile.replace(".zip", "");
                ZipFileUtils.compressZipFile(zipDir, zipFile);
                FileUtils.deleteQuietly(new File(zipDir));
            } catch (IOException e) {
                logger.error("{} processed ip Obfuscator compress zip failed", zipFile, e);
                resultRecorder.addFailedFile(zipFile);
            }
        }
    }

    public static List<String> findFiles(File dirName, Function<File, Boolean> findFunction) {
        List<String> zipFiles = new ArrayList<>();
        if (dirName == null) {
            return zipFiles;
        }
        File[] files = dirName.listFiles();
        if (files == null) {
            return zipFiles;
        }
        for (File subFile : files) {
            if (subFile.isDirectory()) {
                List<String> zipFilePath = findFiles(subFile, findFunction);
                zipFiles.addAll(zipFilePath);
            } else {
                if (Boolean.TRUE.equals(findFunction.apply(subFile))) {
                    zipFiles.add(subFile.getAbsolutePath());
                }
            }
        }
        return zipFiles;
    }
}
