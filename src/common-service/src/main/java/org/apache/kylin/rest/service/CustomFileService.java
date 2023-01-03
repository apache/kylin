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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_JAR_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_JAR_PARSERS_NOT_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_JAR_TOO_LARGE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_LOAD_JAR_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_NOT_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_PARSER_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_UPLOAD_JAR_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_UPLOAD_PARSER_LIMIT;
import static org.apache.kylin.metadata.jar.JarTypeEnum.STREAMING_CUSTOM_PARSER;

import java.io.IOException;
import java.security.AccessController;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.loader.AddToClassPathAction;
import org.apache.kylin.loader.ParserClassLoaderState;
import org.apache.kylin.metadata.jar.JarInfo;
import org.apache.kylin.metadata.jar.JarInfoManager;
import org.apache.kylin.metadata.jar.JarTypeEnum;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.streaming.DataParserInfo;
import org.apache.kylin.metadata.streaming.DataParserManager;
import org.apache.kylin.parser.AbstractDataParser;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class CustomFileService extends BasicService {

    @Autowired
    private AclEvaluate aclEvaluate;

    @SneakyThrows
    public Set<String> uploadJar(MultipartFile jarFile, String project, String jarType) {
        aclEvaluate.checkProjectWritePermission(project);
        JarTypeEnum.validate(jarType);

        checkJarLegal(jarFile, project, jarType);
        return uploadStreamingCustomJar(jarFile, project, jarType);
    }

    public String removeJar(String project, String jarName, String jarType) {
        aclEvaluate.checkProjectWritePermission(project);
        JarTypeEnum.validate(jarType);

        checkJarName(jarName);
        return removeStreamingCustomJar(project, jarName, jarType);
    }

    public Set<String> uploadStreamingCustomJar(MultipartFile jarFile, String project, String jarType)
            throws IOException {
        String jarPath = uploadCustomJar(jarFile, project, jarType);
        return loadParserJar(jarFile.getOriginalFilename(), jarPath, project);
    }

    /**
     * check jar file is legal
     * - jar file end with '.jar'
     * - jar file size < kylin.streaming.custom-jar-size
     * - The jar file does not exist in the metadata
     */
    public void checkJarLegal(MultipartFile jarFile, String project, String jarType) throws IllegalArgumentException {
        String jarName = jarFile.getOriginalFilename();
        checkJarName(jarName);
        long jarMaxSize = NProjectManager.getProjectConfig(project).getStreamingCustomJarSizeMB();
        if (jarFile.getSize() > jarMaxSize) {
            throw new KylinException(CUSTOM_PARSER_JAR_TOO_LARGE, jarMaxSize);
        }
        JarInfo jarInfo = getManager(JarInfoManager.class, project).getJarInfo(JarTypeEnum.valueOf(jarType), jarName);
        if (Objects.nonNull(jarInfo)) {
            throw new KylinException(CUSTOM_PARSER_JAR_EXISTS, jarName);
        }
        log.info("The jar file [{}] can be loaded", jarName);
    }

    /**
     * upload custom jar to hdfs
     */
    public String uploadCustomJar(MultipartFile jarFile, String project, String jarType)
            throws KylinException, IOException {
        String jarName = jarFile.getOriginalFilename();
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String jarHdfsPath = getJarPath(project, jarName, jarType);
        Path path = new Path(jarHdfsPath);
        try (FSDataOutputStream out = fs.create(path)) {
            IOUtils.copyBytes(jarFile.getInputStream(), out, 4096, true);
        } catch (IOException e) {
            // delete hdfs jar file
            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), path);
            throw new KylinException(CUSTOM_PARSER_UPLOAD_JAR_FAILED, e);
        }
        log.info("uploaded jar file [{}] to [{}]", jarName, jarHdfsPath);
        return jarHdfsPath;
    }

    /**
     * check class legal then add parser class to meta
     */
    public Set<String> loadParserJar(String jarName, String jarHdfsPath, String project) throws IOException {
        Set<String> dataParserSet = null;
        try {
            dataParserSet = checkParserLegal(jarName, jarHdfsPath, project);
            // load class to meta
            Set<String> finalDataParserSet = dataParserSet;
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                DataParserManager manager = getManager(DataParserManager.class, project);
                manager.initDefault();
                for (String className : finalDataParserSet) {
                    manager.createDataParserInfo(new DataParserInfo(project, className, jarName));
                }
                return null;
            }, project);
        } catch (Exception e) {
            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(jarHdfsPath));
            ExceptionUtils.rethrow(e);
        }
        return dataParserSet;
    }

    @SneakyThrows
    public Set<String> checkParserLegal(String jarName, String jarPath, String project) {
        Set<String> jarSet = Sets.newHashSet(jarPath);
        Set<String> newParserSet = Sets.newHashSet();

        // try load new jar and get new parsers
        val addAction = new AddToClassPathAction(Thread.currentThread().getContextClassLoader(), jarSet);
        try (val parserClassLoader = AccessController.doPrivileged(addAction)) {
            val loadParsers = ServiceLoader.load(AbstractDataParser.class, parserClassLoader);
            for (val parser : loadParsers) {
                newParserSet.add(parser.getClass().getName());
                log.info("{} get parser: {}", jarName, parser.getClass().getName());
            }
        }

        // new parsers is empty
        if (CollectionUtils.isEmpty(newParserSet)) {
            throw new KylinException(CUSTOM_PARSER_JAR_PARSERS_NOT_EXISTS, jarName);
        }

        // get parsers from meta
        initDefaultParser(project);
        Set<String> existParserSet = getManager(DataParserManager.class, project).listDataParserInfo().stream()
                .map(DataParserInfo::getClassName).collect(Collectors.toSet());

        // check duplicate parser name
        for (String className : newParserSet) {
            if (existParserSet.contains(className)) {
                throw new KylinException(CUSTOM_PARSER_PARSER_EXISTS, jarName, className);
            }
        }

        // check parser count
        int limitCount = NProjectManager.getProjectConfig(project).getStreamingCustomParserLimit();
        int newCount = newParserSet.size();
        int existCount = existParserSet.size() - 1;
        if ((existCount + newCount) > limitCount) {
            throw new KylinException(CUSTOM_PARSER_UPLOAD_PARSER_LIMIT, existCount, newCount, limitCount);
        }

        // try register jar into classloader and meta
        try {
            ParserClassLoaderState.getInstance(project).registerJars(jarSet);
            JarInfo jarInfo = new JarInfo(project, jarName, jarPath, STREAMING_CUSTOM_PARSER);
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                    () -> getManager(JarInfoManager.class, project).createJarInfo(jarInfo), project);
        } catch (Exception e) {
            ParserClassLoaderState.getInstance(project).unregisterJar(jarSet);
            throw new KylinException(CUSTOM_PARSER_LOAD_JAR_FAILED, e);
        }
        return newParserSet;
    }

    /**
     * To delete a jar
     * first check whether the class is referenced by the table. If so, it cannot be deleted
     */
    @SneakyThrows
    public String removeStreamingCustomJar(String project, String jarName, String jarType) {
        JarTypeEnum.validate(jarType);
        String jarPath = getJarPath(project, jarName, jarType);
        // remove from meta
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getManager(DataParserManager.class, project).removeJar(jarName);
            getManager(JarInfoManager.class, project).removeJarInfo(JarTypeEnum.valueOf(jarType), jarName);
            return null;
        }, project);
        // remove from classloader
        ParserClassLoaderState.getInstance(project).unregisterJar(Sets.newHashSet(jarPath));
        // delete hdfs jar file
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(jarPath));
        log.info("remove jar {} success", jarPath);
        return jarName;
    }

    private static String getJarPath(String project, String jarFileName, String jarType) {
        return KylinConfig.getInstanceFromEnv().getHdfsCustomJarPath(project, jarType) + jarFileName;
    }

    private void checkJarName(String jarName) {
        if (StringUtils.endsWith(jarName, ".jar")) {
            return;
        }
        throw new KylinException(CUSTOM_PARSER_NOT_JAR, jarName);
    }
}
