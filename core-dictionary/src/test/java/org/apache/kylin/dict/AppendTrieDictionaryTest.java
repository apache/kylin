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

package org.apache.kylin.dict;

import static org.apache.kylin.dict.global.GlobalDictHDFSStore.V2_INDEX_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.dict.global.AppendDictSliceKey;
import org.apache.kylin.dict.global.AppendTrieDictionaryBuilder;
import org.apache.kylin.dict.global.GlobalDictHDFSStore;
import org.apache.kylin.dict.global.GlobalDictMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.testing.EqualsTester;

public class AppendTrieDictionaryTest extends LocalFileMetadataTestCase {
    private static final String RESOURCE_DIR = "/dict/append_dict_test/" + RandomUtil.randomUUID();
    private static String BASE_DIR;
    private static String LOCAL_BASE_DIR;

    @Before
    public void beforeTest() throws IOException {
        staticCreateTestMetadata();
        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.append-entry-size", "50000");
        BASE_DIR = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/resources/GlobalDict" + RESOURCE_DIR
                + "/";
        LOCAL_BASE_DIR = getLocalWorkingDirectory() + "/resources/GlobalDict" + RESOURCE_DIR + "/";
    }

    @After
    public void afterTest() {
        cleanup();
        staticCleanupTestMetadata();
    }

    private void cleanup() {
        Path basePath = new Path(BASE_DIR);
        try {
            HadoopUtil.getFileSystem(basePath).delete(basePath, true);
        } catch (IOException e) {
        }
    }

    private static final String[] words = new String[] { "paint", "par", "part", "parts", "partition", "partitions",
            "party", "partie", "parties", "patient", "taste", "tar", "trie", "try", "tries", "字典", "字典树", "字母", // non-ascii characters
            "", // empty
            "paiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
            "paiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiipaiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
            "paintjkjdfklajkdljfkdsajklfjklsadjkjekjrklewjrklewjklrjklewjkljkljkljkljweklrjewkljrklewjrlkjewkljrkljkljkjlkjjkljkljkljkljlkjlkjlkjljdfadfads"
                    + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk"
                    + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk"
                    + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk"
                    + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk"
                    + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk"
                    + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk"
                    + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk",
            "paint", "tar", "try", // some dup
    };

    private AppendTrieDictionaryBuilder createBuilder() throws IOException {
        int maxEntriesPerSlice = KylinConfig.getInstanceFromEnv().getAppendDictEntrySize();
        return new AppendTrieDictionaryBuilder(BASE_DIR, maxEntriesPerSlice, true);
    }

    @Test
    public void testStringRepeatly() throws IOException {
        ArrayList<String> list = new ArrayList<>();
        Collections.addAll(list, words);
        ArrayList<String> notfound = new ArrayList<>();
        notfound.add("pa");
        notfound.add("pars");
        notfound.add("tri");
        notfound.add("字");
        for (int i = 0; i < 50; i++) {
            testStringDictAppend(list, notfound, true);
            //to speed up the test
            cleanup();
        }
    }

    @Test
    public void testEnglishWords() throws Exception {
        InputStream is = new FileInputStream("src/test/resources/dict/english-words.80 (scowl-2015.05.18).txt");
        ArrayList<String> str = loadStrings(is);
        testStringDictAppend(str, null, false);
    }

    @Test
    public void testCategoryNames() throws Exception {
        InputStream is = new FileInputStream("src/test/resources/dict/dw_category_grouping_names.dat");
        ArrayList<String> str = loadStrings(is);
        testStringDictAppend(str, null, true);
    }

    private static ArrayList<String> loadStrings(InputStream is) throws Exception {
        ArrayList<String> r = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        try {
            String word;
            while ((word = reader.readLine()) != null) {
                word = word.trim();
                if (word.isEmpty() == false)
                    r.add(word);
            }
        } finally {
            reader.close();
            is.close();
        }
        return r;
    }

    @Ignore("need huge key set")
    @Test
    public void testHugeKeySet() throws IOException {
        AppendTrieDictionaryBuilder builder = createBuilder();

        AppendTrieDictionary<String> dict = null;

        InputStream is = new FileInputStream("src/test/resources/dict/huge_key");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        try {
            String word;
            while ((word = reader.readLine()) != null) {
                word = word.trim();
                if (!word.isEmpty())
                    builder.addValue(word);
            }
        } finally {
            reader.close();
            is.close();
        }
        dict = builder.build(0);
        dict.dump(System.out);
    }

    private void testStringDictAppend(ArrayList<String> list, ArrayList<String> notfound, boolean shuffleList)
            throws IOException {
        Random rnd = new Random(System.currentTimeMillis());
        ArrayList<String> strList = new ArrayList<String>();
        strList.addAll(list);
        if (shuffleList) {
            Collections.shuffle(strList, rnd);
        }
        BytesConverter converter = new StringBytesConverter();

        AppendTrieDictionaryBuilder b = createBuilder();

        TreeMap<Integer, String> checkMap = new TreeMap<>();
        int firstAppend = rnd.nextInt(strList.size() / 2);
        int secondAppend = firstAppend + rnd.nextInt((strList.size() - firstAppend) / 2);
        int appendIndex = 0;
        int checkIndex = 0;

        for (; appendIndex < firstAppend; appendIndex++) {
            b.addValue(strList.get(appendIndex));
        }
        AppendTrieDictionary<String> dict = b.build(0);
        dict.dump(System.out);
        for (; checkIndex < firstAppend; checkIndex++) {
            String str = strList.get(checkIndex);
            byte[] bytes = converter.convertToBytes(str);
            int id = dict.getIdFromValueBytesWithoutCache(bytes, 0, bytes.length, 0);
            assertNotEquals(String.format(Locale.ROOT, "Value %s not exist", str), -1, id);
            assertFalse(
                    String.format(Locale.ROOT, "Id %d for %s should be empty, but is %s", id, str, checkMap.get(id)),
                    checkMap.containsKey(id) && !str.equals(checkMap.get(id)));
            checkMap.put(id, str);
        }

        // reopen dict and append
        b = createBuilder();

        for (; appendIndex < secondAppend; appendIndex++) {
            b.addValue(strList.get(appendIndex));
        }
        AppendTrieDictionary<String> newDict = b.build(0);
        assert newDict.equals(dict);
        dict = newDict;
        dict.dump(System.out);
        checkIndex = 0;
        for (; checkIndex < secondAppend; checkIndex++) {
            String str = strList.get(checkIndex);
            byte[] bytes = converter.convertToBytes(str);
            int id = dict.getIdFromValueBytesWithoutCache(bytes, 0, bytes.length, 0);
            assertNotEquals(String.format(Locale.ROOT, "Value %s not exist", str), -1, id);
            if (checkIndex < firstAppend) {
                assertEquals("Except id " + id + " for " + str + " but " + checkMap.get(id), str, checkMap.get(id));
            } else {
                // check second append str, should be new id
                assertFalse(String.format(Locale.ROOT, "Id %d for %s should be empty, but is %s", id, str,
                        checkMap.get(id)), checkMap.containsKey(id) && !str.equals(checkMap.get(id)));
                checkMap.put(id, str);
            }
        }

        // reopen dict and append rest str
        b = createBuilder();

        for (; appendIndex < strList.size(); appendIndex++) {
            b.addValue(strList.get(appendIndex));
        }
        newDict = b.build(0);
        assert newDict.equals(dict);
        dict = newDict;
        dict.dump(System.out);
        checkIndex = 0;
        for (; checkIndex < strList.size(); checkIndex++) {
            String str = strList.get(checkIndex);
            byte[] bytes = converter.convertToBytes(str);
            int id = dict.getIdFromValueBytesWithoutCache(bytes, 0, bytes.length, 0);
            assertNotEquals(String.format(Locale.ROOT, "Value %s not exist", str), -1, id);
            if (checkIndex < secondAppend) {
                assertEquals("Except id " + id + " for " + str + " but " + checkMap.get(id), str, checkMap.get(id));
            } else {
                // check third append str, should be new id
                assertFalse(String.format(Locale.ROOT, "Id %d for %s should be empty, but is %s", id, str,
                        checkMap.get(id)), checkMap.containsKey(id) && !str.equals(checkMap.get(id)));
                checkMap.put(id, str);
            }
        }
        if (notfound != null) {
            for (String s : notfound) {
                byte[] bytes = converter.convertToBytes(s);
                int id = dict.getIdFromValueBytesWithoutCache(bytes, 0, bytes.length, 0);
                assertEquals(-1, id);
            }
        }

        dict = testSerialize(dict, converter);
        for (String str : strList) {
            byte[] bytes = converter.convertToBytes(str);
            int id = dict.getIdFromValueBytesWithoutCache(bytes, 0, bytes.length, 0);
            assertNotEquals(String.format(Locale.ROOT, "Value %s not exist", str), -1, id);
            assertEquals("Except id " + id + " for " + str + " but " + checkMap.get(id), str, checkMap.get(id));
        }
    }

    private static AppendTrieDictionary<String> testSerialize(AppendTrieDictionary<String> dict,
            BytesConverter converter) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream dataout = new DataOutputStream(bout);
            dict.write(dataout);
            dataout.close();
            ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
            DataInputStream datain = new DataInputStream(bin);
            AppendTrieDictionary<String> r = new AppendTrieDictionary<String>();
            r.readFields(datain);
            datain.close();
            return r;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMaxInteger() throws IOException {
        AppendTrieDictionaryBuilder builder = createBuilder();
        builder.setMaxId(Integer.MAX_VALUE - 2);
        builder.addValue("a");
        builder.addValue("ab");
        builder.addValue("acd");
        builder.addValue("ac");
        AppendTrieDictionary dict = builder.build(0);
        assertEquals(2147483646, dict.getIdFromValue("a", 0));
        assertEquals(2147483647, dict.getIdFromValue("ab", 0));
        assertEquals(-2147483647, dict.getIdFromValue("ac", 0));
        assertEquals(-2147483648, dict.getIdFromValue("acd", 0));
    }

    @Ignore("Only occurred when value is very long (>8000 bytes)")
    @Test
    public void testSuperLongValue() throws IOException {
        AppendTrieDictionaryBuilder builder = createBuilder();
        String value = "a";
        for (int i = 0; i < 10000; i++) {
            value += "a";
            try {
                builder.addValue(value);
            } catch (StackOverflowError e) {
                System.out.println("\nstack overflow " + i);
                throw e;
            }
        }
        AppendTrieDictionary dictionary = builder.build(0);
        dictionary.getMaxId();
    }

    @Test
    public void testSplitContainSuperLongValue() throws IOException {
        String superLongValue = "%5Cx1A%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%7E%29%5CxEF%5CxBF%5CxBD%5Cx1B+%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5Cx13%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5B";

        createAppendTrieDict(Arrays.asList("a", superLongValue));
    }

    @Test
    public void testSuperLongValueAsFileName() throws IOException {
        String superLongValue = "%5Cx1A%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%7E%29%5CxEF%5CxBF%5CxBD%5Cx1B+%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5Cx13%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5CxEF%5CxBF%5CxBD%5B";

        createAppendTrieDict(Arrays.asList("a", superLongValue));
    }

    @Test
    public void testIllegalFileNameValue() throws IOException {
        createAppendTrieDict(Arrays.asList("::", ":"));
    }

    @Test
    public void testSkipAddValue() throws IOException {
        createAppendTrieDict(new ArrayList<String>());
    }

    @Test
    public void testSerialize() throws IOException {
        AppendTrieDictionaryBuilder builder = createBuilder();
        AppendTrieDictionary dict = builder.build(0);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dataout = new DataOutputStream(bout);
        dict.write(dataout);
        dataout.close();
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        DataInputStream datain = new DataInputStream(bin);

        assertNull(new Path(datain.readUTF()).toUri().getScheme());
        datain.close();
    }

    @Test
    public void testDeserialize() throws IOException {
        AppendTrieDictionaryBuilder builder = createBuilder();
        builder.setMaxId(Integer.MAX_VALUE - 2);
        builder.addValue("a");
        builder.addValue("ab");
        List<String> strList = Lists.newArrayList("a", "ab");
        AppendTrieDictionary dict = builder.build(0);
        TreeMap checkMap = new TreeMap();
        BytesConverter converter = new StringBytesConverter();
        for (String str : strList) {
            byte[] bytes = converter.convertToBytes(str);
            int id = dict.getIdFromValueBytesWithoutCache(bytes, 0, bytes.length, 0);
            checkMap.put(id, str);
        }
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dataout = new DataOutputStream(bout);
        dict.setSaveAbsolutePath(true);
        dict.write(dataout);
        dataout.close();
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        DataInputStream datain = new DataInputStream(bin);
        AppendTrieDictionary<String> r = new AppendTrieDictionary<String>();
        r.readFields(datain);
        datain.close();

        for (String str : strList) {
            byte[] bytes = converter.convertToBytes(str);
            int id = r.getIdFromValueBytesWithoutCache(bytes, 0, bytes.length, 0);
            assertNotEquals(String.format(Locale.ROOT, "Value %s not exist", str), -1, id);
            assertEquals("Except id " + id + " for " + str + " but " + checkMap.get(id), str, checkMap.get(id));
        }
    }

    private void createAppendTrieDict(List<String> valueList) throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.append-entry-size", "1");

        AppendTrieDictionaryBuilder builder = createBuilder();

        for (String value : valueList) {
            builder.addValue(value);
        }

        builder.build(0);
    }

    private static class CachedFileFilter implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            return pathname.getName().startsWith("cached_");
        }
    }

    private static class VersionFilter implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            return pathname.getName().startsWith(GlobalDictHDFSStore.VERSION_PREFIX);
        }
    }

    @Test
    public void testMultiVersions() throws IOException, InterruptedException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.append-entry-size", "4");

        AppendTrieDictionaryBuilder builder = createBuilder();
        builder.addValue("a");
        builder.addValue("b");
        builder.addValue("c");
        builder.addValue("d");
        builder.addValue("e");
        builder.addValue("f");
        AppendTrieDictionary dict = builder.build(0);

        assertEquals(2, dict.getIdFromValue("b"));

        // re-open dict, append new data
        builder = createBuilder();
        builder.addValue("g");

        // new data is not visible
        try {
            dict.getIdFromValue("g");
            fail("Value 'g' (g) not exists!");
        } catch (IllegalArgumentException e) {

        }

        // append data, and be visible for new immutable map
        builder.addValue("h");

        AppendTrieDictionary newDict = builder.build(0);
        assert newDict.equals(dict);

        assertEquals(7, newDict.getIdFromValue("g"));
        assertEquals(8, newDict.getIdFromValue("h"));

        // Check versions retention
        File dir = new File(LOCAL_BASE_DIR);
        assertEquals(2, dir.listFiles(new VersionFilter()).length);
    }

    @Test
    public void testVersionRetention() throws IOException, InterruptedException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.append-entry-size", "4");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.append-max-versions", "1");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.append-version-ttl", "1000");

        AppendTrieDictionaryBuilder builder = createBuilder();
        builder.addValue("a");

        //version 1
        builder.build(0);

        // Check versions retention
        File dir = new File(LOCAL_BASE_DIR);
        assertEquals(1, dir.listFiles(new VersionFilter()).length);

        // sleep to make version 1 expired
        Thread.sleep(1200);

        //version 2
        builder = createBuilder();
        builder.addValue("");
        builder.build(0);

        // Check versions retention
        assertEquals(1, dir.listFiles(new VersionFilter()).length);
    }

    @Test
    public void testOldDirFormat() throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.append-entry-size", "4");

        AppendTrieDictionaryBuilder builder = createBuilder();
        builder.addValue("a");
        builder.addValue("b");
        builder.addValue("c");
        builder.addValue("d");
        builder.addValue("e");
        builder.addValue("f");
        builder.build(0);

        convertDirToOldFormat(BASE_DIR);

        File dir = new File(LOCAL_BASE_DIR);
        assertEquals(0, dir.listFiles(new VersionFilter()).length);
        assertEquals(3, dir.listFiles(new CachedFileFilter()).length);

        //convert older format to new format when builder init
        builder = createBuilder();
        builder.build(0);

        assertEquals(1, dir.listFiles(new VersionFilter()).length);
    }

    private void convertDirToOldFormat(String baseDir) throws IOException {
        Path basePath = new Path(baseDir);
        FileSystem fs = HadoopUtil.getFileSystem(basePath);

        // move version dir to base dir, to simulate the older format
        GlobalDictHDFSStore store = new GlobalDictHDFSStore(baseDir);
        Long[] versions = store.listAllVersions();
        Path versionPath = store.getVersionDir(versions[versions.length - 1]);
        Path tmpVersionPath = new Path(versionPath.getParent().getParent(), versionPath.getName());
        fs.rename(versionPath, tmpVersionPath);
        fs.delete(new Path(baseDir), true);
        fs.rename(tmpVersionPath, new Path(baseDir));
    }

    @Test
    public void testOldIndexFormat() throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.append-entry-size", "4");

        AppendTrieDictionaryBuilder builder = createBuilder();
        builder.addValue("a");
        builder.addValue("b");
        builder.addValue("c");
        builder.addValue("d");
        builder.addValue("e");
        builder.addValue("f");
        builder.build(0);

        convertIndexToOldFormat(BASE_DIR);

        builder = createBuilder();
        builder.addValue("g");
        builder.addValue("h");
        builder.addValue("i");
        AppendTrieDictionary dict = builder.build(0);

        assertEquals(1, dict.getIdFromValue("a"));
        assertEquals(7, dict.getIdFromValue("g"));
    }

    private void convertIndexToOldFormat(String baseDir) throws IOException {
        Path basePath = new Path(baseDir);
        FileSystem fs = HadoopUtil.getFileSystem(basePath);

        GlobalDictHDFSStore store = new GlobalDictHDFSStore(baseDir);
        Long[] versions = store.listAllVersions();
        GlobalDictMetadata metadata = store.getMetadata(versions[versions.length - 1]);

        //convert v2 index to v1 index
        Path versionPath = store.getVersionDir(versions[versions.length - 1]);
        Path v2IndexFile = new Path(versionPath, V2_INDEX_NAME);

        fs.delete(v2IndexFile, true);
        GlobalDictHDFSStore.IndexFormat indexFormatV1 = new GlobalDictHDFSStore.IndexFormatV1(fs,
                HadoopUtil.getCurrentConfiguration());
        indexFormatV1.writeIndexFile(versionPath, metadata);

        //convert v2 fileName format to v1 fileName format
        for (Map.Entry<AppendDictSliceKey, String> entry : metadata.sliceFileMap.entrySet()) {
            fs.rename(new Path(versionPath, entry.getValue()), new Path(versionPath, "cached_" + entry.getKey()));
        }
    }

    @Test
    public void testTooManySliceEvictions() throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.max-cache-size", "3");
        AppendTrieDictionaryBuilder builder = createBuilder();
        for (int i = 0; i < 100000; i++) {
            builder.addValue(Integer.toString(i));
        }
        AppendTrieDictionary dict = builder.build(0);

        assertEquals(4, dict.getDictMetadata().sliceFileMap.size());
        assertEquals(1, dict.getIdFromValue("0", 0));
        assertEquals(0, dict.getCacheStats().evictionCount());
        assertEquals(1, dict.getCacheStats().loadCount());

        List<String> keys = new ArrayList<>(100000);
        for (int i = 0; i < 100000; i++) {
            keys.add(Integer.toString(i));
        }
        Collections.sort(keys);
        for (String key : keys) {
            assertEquals(Integer.parseInt(key) + 1, dict.getIdFromValue(key, 0));
        }
        assertEquals(1, dict.getCacheStats().evictionCount());
        assertEquals(4, dict.getCacheStats().loadCount());

        // out of order
        Collections.shuffle(keys);
        try {
            for (String key : keys) {
                assertEquals(Integer.parseInt(key) + 1, dict.getIdFromValue(key, 0));
            }
            assertFalse("Should throw RuntimeException for too many dict slice evictions", true);
        } catch (RuntimeException e) {
            assertEquals("Too many dict slice evictions", e.getMessage().substring(0, 29));
        }
        assertEquals(22, dict.getCacheStats().evictionCount());
        assertEquals(25, dict.getCacheStats().loadCount());

        KylinConfig.getInstanceFromEnv().setProperty("kylin.dictionary.max-cache-size", "-1");
    }

    @Test
    public void testEqualsAndHashCode() throws IOException {
        AppendTrieDictionaryBuilder builder = createBuilder();
        AppendTrieDictionary dict1 = builder.build(0);
        AppendTrieDictionary dict2 = builder.build(1);

        int maxEntriesPerSlice = KylinConfig.getInstanceFromEnv().getAppendDictEntrySize();
        String tempBaseDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/resources/GlobalDict"
                + "/dict/append_dict_test/" + RandomUtil.randomUUID() + "/";
        AppendTrieDictionaryBuilder builder2 = new AppendTrieDictionaryBuilder(tempBaseDir, maxEntriesPerSlice, true);
        AppendTrieDictionary dict3 = builder2.build(0);
        AppendTrieDictionary dict4 = builder2.build(1);

        new EqualsTester().addEqualityGroup(dict1, dict2).addEqualityGroup(dict3, dict4).testEquals();
    }

    @Test
    public void testToString() {
        AppendTrieDictionary dict = createDefaultTestDict();
        assertEquals("AppendTrieDictionary(" + BASE_DIR + ")", dict.toString());
    }

    @Test
    public void testContains() {
        AppendTrieDictionary dict = createDefaultTestDict();
        assertEquals(false, dict.contains(dict));
    }

    @Test
    public void testGetMinId() {
        AppendTrieDictionary dict = createDefaultTestDict();
        assertEquals(0, dict.getMinId());
    }

    @Test
    public void testGetMaxId() throws IOException {
        AppendTrieDictionaryBuilder builder = createBuilder();
        builder.addValue("a");
        builder.addValue("b");
        builder.addValue("c");
        builder.addValue("d");
        builder.addValue("e");
        builder.addValue("f");
        AppendTrieDictionary dict = builder.build(0);
        assertEquals(6, dict.getMaxId());
    }

    @Test
    public void testGetSizeOfId() {
        AppendTrieDictionary dict = createDefaultTestDict();
        assertEquals(4, dict.getSizeOfId());
    }

    @Test
    public void testGetSizeOfValue() throws IOException {
        AppendTrieDictionaryBuilder builder = createBuilder();
        builder.addValue("a");
        builder.addValue("aaaaaa");
        AppendTrieDictionary dict = builder.build(0);
        assertEquals(6, dict.getSizeOfValue());
    }

    private AppendTrieDictionary createDefaultTestDict() {
        try {
            return createBuilder().build(0);
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare dict for testing.");
        }

    }
}
