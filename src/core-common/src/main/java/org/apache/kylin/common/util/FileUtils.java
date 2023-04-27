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

package org.apache.kylin.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;

public final class FileUtils {
    public static File findFile(String dir, String ptn) {
        File[] files = new File(dir).listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.getName().matches(ptn))
                    return f;
            }
        }
        return null;
    }

    public static List<File> findFiles(String dir, String ptn) {
        File[] files = new File(dir).listFiles();
        if (files != null) {
            return Arrays.stream(files).filter(f -> f.getName().matches(ptn)).collect(Collectors.toList());
        }
        return Lists.newArrayList();
    }

    public static List<File> findFiles(String dir) {
        File[] files = new File(dir).listFiles();
        if (files != null) {
            return Lists.newArrayList(files);
        }
        return Lists.newArrayList();
    }

    public static Map<String, String> readFromPropertiesFile(File file) {
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            return readFromPropertiesFile(fileInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> readFromPropertiesFile(InputStream inputStream) {
        try (BufferedReader confReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
            OrderedProperties temp = new OrderedProperties();
            temp.load(confReader);
            return temp.getProperties();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static String encodeBase64File(String path) throws Exception {
        File file = new File(path);
        try (FileInputStream inputFile = new FileInputStream(file)) {
            byte[] buffer = new byte[(int) file.length()];
            final int read = inputFile.read(buffer);
            Preconditions.checkState(read != -1);
            return Base64.encodeBase64String(buffer);
        }
    }

    public static void decoderBase64File(String base64Code, String targetPath) throws Exception {
        val buffer = Base64.decodeBase64(base64Code);
        try (FileOutputStream out = new FileOutputStream(targetPath)) {
            out.write(buffer);
        }
    }
}
