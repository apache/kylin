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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

public class ClasspathScanner {

    public static void main(final String[] args) {
        ClasspathScanner scanner = new ClasspathScanner();

        if (args.length == 0) {
            for (File f : scanner.rootResources) {
                System.out.println(f.getAbsolutePath());
            }
            System.exit(0);
        }
        
        final int[] hitCount = new int[1];
        
        scanner.scan("", new ResourceVisitor() {
            public void accept(File dir, String relativeFileName) {
                check(dir.getAbsolutePath(), relativeFileName.replace('\\', '/'));
            }

            public void accept(ZipFile archive, ZipEntry zipEntry) {
                check(archive.getName(), zipEntry.getName().replace('\\', '/'));
            }

            private void check(String base, String relativePath) {
                boolean hit = false;
                for (int i = 0; i < args.length && !hit; i++) {
                    hit = relativePath.endsWith(args[i]) || match(args[i], relativePath);
                }

                if (hit) {
                    System.out.println(base + " - " + relativePath);
                    hitCount[0]++;
                }
            }
        });
        
        int exitCode = hitCount[0] > 0 ? 0 : 1;
        System.exit(exitCode);
    }

    /**
     * Scan classpath to find resources that has a suffix in name.
     * 
     * This thread's context class loader is used to define the searching
     * classpath.
     * 
     * @param suffix
     *            like ".jsp" for example
     * @return a string array; each element is a standard resource name relative
     *         to the root of class path, like "young/web/frame.jsp"
     */
    public static String[] findResources(final String suffix) {
        ClasspathScanner scanner = new ClasspathScanner();

        final ArrayList result = new ArrayList();

        scanner.scan(suffix, new ResourceVisitor() {
            public void accept(File dir, String relativeFileName) {
                result.add(relativeFileName.replace('\\', '/'));
            }

            public void accept(ZipFile archive, ZipEntry zipEntry) {
                result.add(zipEntry.getName().replace('\\', '/'));
            }
        });

        return (String[]) result.toArray(new String[result.size()]);
    }

    // ============================================================================

    private File[] rootResources;

    public ClasspathScanner() {
        this(Thread.currentThread().getContextClassLoader(), true);
    }

    public ClasspathScanner(ClassLoader classLoader, boolean recursive) {
        this.rootResources = extractRoots(classLoader, recursive);
    }

    static File[] extractRoots(ClassLoader loader, boolean recursive) {
        List<ClassLoader> loaders = new ArrayList<>();
        while (loader != null) {
            loaders.add(loader);
            if (!recursive)
                break;
            loader = loader.getParent();
        }
        
        List<File> roots = new ArrayList();
        
        // parent first
        for (int i = loaders.size() - 1; i >= 0; i--) {
            ClassLoader l = loaders.get(i);
            if (l instanceof URLClassLoader) {
                for (URL url : ((URLClassLoader) l).getURLs()) {
                    // tricky: space is "%20" in URL
                    File f = new File(url.getFile().replace("%20", " "));
                    roots.add(f);
                }
            }
        }

        return (File[]) roots.toArray(new File[roots.size()]);
    }

    /**
     * @param rootResources
     *            can be directories or zip/jar archives. All contents inside
     *            will be searched.
     */
    public ClasspathScanner(File[] rootResources) {
        this.rootResources = rootResources;
    }

    public static interface ResourceVisitor {

        void accept(File dir, String relativeFileName);

        void accept(ZipFile archive, ZipEntry zipEntry);
    }

    public void scan(String suffix, ResourceVisitor visitor) {
        for (int i = 0; i < rootResources.length; i++) {
            if (rootResources[i].exists()) {
                if (rootResources[i].isDirectory()) {
                    scanDirectory(rootResources[i], suffix, visitor);
                } else if (rootResources[i].getName().contains(".zip") || rootResources[i].getName().contains(".jar")) {
                    scanArchive(rootResources[i], suffix, visitor);
                }
            }
        }
    }

    private void scanArchive(File archive, String suffix, ResourceVisitor visitor) {
        ZipFile zip = null;
        try {
            zip = new ZipFile(archive, ZipFile.OPEN_READ);
            Enumeration enu = zip.entries();
            while (enu.hasMoreElements()) {
                ZipEntry entry = (ZipEntry) enu.nextElement();
                if (entry.getName().endsWith(suffix)) {
                    visitor.accept(zip, entry);
                }
            }
        } catch (ZipException e) {
            // e.printStackTrace();
        } catch (IOException e) {
            // e.printStackTrace();
        } finally {
            if (zip != null) {
                try {
                    zip.close();
                } catch (IOException e) {
                    // e.printStackTrace();
                }
            }
        }
    }

    private void scanDirectory(File dir, String suffix, ResourceVisitor visitor) {
        String[] files = scanFiles(dir, new String[] { "*" + suffix });
        for (int i = 0; i < files.length; i++) {
            if (files[i].endsWith(suffix)) {
                visitor.accept(dir, files[i]);
            }
        }
    }

    /**
     * Search files inside a dir that match certain patterns.
     * @param dir from where files are searched
     * @param includes patterns that may contain '*' or '?' wild char
     * @return an array of included files relative to the dir
     */
    public static String[] scanFiles(File dir, String... includes) {
        return scanFiles(dir, includes, null);
    }

    /**
     * Search files inside a dir that match any of the include patterns
     * and not match the exclude patterns. All files are included if 
     * include patterns is null/empty; no files are excluded if exclude
     * patterns is null/empty.
     * @param dir from where files are searched
     * @param includes patterns that may contain '*' or '?' wild char
     * @param excludes patterns that may contain '*' or '?' wild char
     * @return an array of included files relative to the dir
     */
    public static String[] scanFiles(File dir, String[] includes, String[] excludes) {
        // remove tailing '/' in patterns
        if (includes != null) {
            for (int i = 0; i < includes.length; i++)
                includes[i] = StringUtil.trimSuffix(includes[i], "/");
        }
        if (excludes != null) {
            for (int i = 0; i < excludes.length; i++)
                excludes[i] = StringUtil.trimSuffix(excludes[i], "/");
        }

        ArrayList result = new ArrayList();
        ArrayList queue = new ArrayList();
        queue.add("");

        String dirPath, path;
        File dirFile, f;
        File[] files;
        while (!queue.isEmpty()) {
            dirPath = (String) queue.remove(queue.size() - 1);
            dirFile = dirPath.length() == 0 ? dir : new File(dir, dirPath);
            files = dirFile.listFiles();
            for (int i = 0; files != null && i < files.length; i++) {
                f = files[i];
                path = dirPath + (dirPath.length() == 0 ? "" : "/") + f.getName();
                if (f.isDirectory()) {
                    // cut off excluded dir early
                    if (scanFiles_isIncluded(path, null, excludes))
                        queue.add(path);
                } else if (f.isFile()) {
                    if (scanFiles_isIncluded(path, includes, excludes))
                        result.add(path);
                }
            }
        }
        return (String[]) result.toArray(new String[result.size()]);
    }

    private static boolean scanFiles_isIncluded(String path, String[] includes, String[] excludes) {
        // if null, means include everything
        if (includes != null && includes.length != 0) {
            boolean included = false;
            for (int i = 0; !included && i < includes.length; i++) {
                if (match(includes[i], path) || match(includes[i] + "/*", path))
                    included = true;
            }
            if (!included)
                return false;
        }
        // if null, means exclude nothing
        if (excludes != null && excludes.length != 0) {
            for (int i = 0; i < excludes.length; i++) {
                if (match(excludes[i], path))
                    return false;
            }
        }
        return true;
    }

    /**
     * Matches a string against a pattern. The pattern contains two special
     * characters: '*' which means zero or more characters, '?' which means one
     * and only one character.
     * 
     * @param pattern
     *            the (non-null) pattern to match against
     * @param str
     *            the (non-null) string that must be matched against the pattern
     * 
     * @return <code>true</code> when the string matches against the pattern,
     *         <code>false</code> otherwise.
     */
    public static boolean match(String pattern, String str) {
        int i = 0, j = 0, ii = 0, jj = 0, plen = pattern.length(), slen = str.length();
        int wordStart, wordEnd, lastPossible;
        while (i < plen) {
            // find the next word in pattern
            wordStart = i;
            while (wordStart < plen && pattern.charAt(wordStart) == '*')
                wordStart++;
            if (wordStart == plen) // all left in pattern is '*'
                return true;
            // find the word end
            wordEnd = wordStart + 1;
            while (wordEnd < plen && pattern.charAt(wordEnd) != '*')
                wordEnd++;

            // locate the word in string
            lastPossible = slen - (wordEnd - wordStart) + 1;
            for (; j < lastPossible; j++) {
                // check if word matches at j
                for (ii = wordStart, jj = j; ii < wordEnd; ii++, jj++)
                    if (pattern.charAt(ii) != '?' && pattern.charAt(ii) != str.charAt(jj))
                        break;
                if (ii == wordEnd) // matched at j
                    break;

                // if there's no '*' before word, then the string must
                // match at j, but it didn't
                if (wordStart == i)
                    return false;
            }

            // failed to locate the word
            if (!(j < lastPossible))
                return false;

            // proceed to the next word
            i = ii;
            j = jj;
        }

        // pattern ended, if string also ended, then it's a match;
        // otherwise it's a mismatch
        return j == slen;
    }

}