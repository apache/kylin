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

package org.apache.kylin.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;

public class LogInitializer {

    public static final String SLASH_SEPARATOR = "/";

    public static void init() {

        File tmp = null;
        try {
            Class<?> logManagerClz = Class.forName("org.apache.logging.log4j.LogManager");
            Class<?> logContextClz = Class.forName("org.apache.logging.log4j.core.LoggerContext");

            String classPath = new File(
                    KylinConnection.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            String separator = String.valueOf(File.separatorChar);
            String configFileName = classPath.substring(0, classPath.lastIndexOf(separator) + 1)
                    + "kylin-jdbc.properties";
            Properties properties = new Properties();
            if (new File(configFileName).exists()) {
                properties.load(preprocessPropertiesFile(configFileName));
                String level = properties.getProperty("LogLevel");
                String file = properties.getProperty("LogPath");
                if (!SLASH_SEPARATOR.equals(separator)) {
                    file = file.replaceAll(Matcher.quoteReplacement(File.separator), "/");
                }
                String maxBackupIndex = properties.getProperty("MaxBackupIndex");
                String maxFileSize = properties.getProperty("MaxFileSize");

                tmp = File.createTempFile("KylinJDBCDRiver", "xml");
                try (PrintStream out = new PrintStream(Files.newOutputStream(tmp.toPath()), false,
                        Charset.defaultCharset().name())) {
                    out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    out.println("<Configuration>");
                    out.println("  <Appenders>");
                    out.println("    <RollingFile name=\"RollingFile\" fileName=\"" + file + "\" filePattern=\"" + file
                            + ".%i\">");
                    out.println("      <PatternLayout>");
                    out.println("        <pattern>%d{ISO8601} %-5p [%t] : %m%n</pattern>");
                    out.println("      </PatternLayout>");
                    out.println("      <Policies>");
                    out.println("        <SizeBasedTriggeringPolicy size=\"" + maxFileSize + "\"/>");
                    out.println("      </Policies>");
                    out.println("      <DefaultRolloverStrategy max=\"" + maxBackupIndex + "\"/>");
                    out.println("    </RollingFile>");
                    out.println("  </Appenders>");
                    out.println("  <Loggers>");
                    out.println("    <Logger name=\"org.apache.kylin.jdbc.shaded\" level=\"off\"/>");
                    out.println("    <Root level=\"" + level + "\">");
                    out.println("      <AppenderRef ref=\"RollingFile\"/>");
                    out.println("    </Root>");
                    out.println("  </Loggers>");
                    out.println("</Configuration>");
                }

                Object context = logManagerClz.getMethod("getContext", boolean.class).invoke(null, false);
                logContextClz.getMethod("setConfigLocation", java.net.URI.class).invoke(context, tmp.toURI());
            }
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            System.out.println("Log4j class not found, fallback to sl4j");
        } catch (Exception e) {
            System.err.println("Failed to init logger");
            e.printStackTrace(System.err);
        } finally {
            if (tmp != null && tmp.exists()) {
                if (tmp.delete()) {
                    System.err.println("Failed to delete temp KylinJDBCDRiver.xml file.");
                }
            }
        }
    }

    private static InputStream preprocessPropertiesFile(String myFile) throws IOException {
        try (Scanner in = new Scanner(new FileReader(myFile))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            while (in.hasNext()) {
                out.write(in.nextLine().replace("\\", "\\\\").getBytes(Charset.defaultCharset()));
                out.write("\n".getBytes(Charset.defaultCharset()));
            }
            return new ByteArrayInputStream(out.toByteArray());
        }
    }
}
