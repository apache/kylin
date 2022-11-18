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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;

public class QueryCLI {

    @SuppressWarnings("static-access")
    private static final Option OPT_SERVICE = OptionBuilder.withArgName("host:port").hasArg().isRequired()
            .withDescription("Service endpoint, e.g. -s localhost:7070").create("s");

    @SuppressWarnings("static-access")
    private static final Option OPT_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired()
            .withDescription("Project name, e.g. -project learn_kylin").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPT_USER = OptionBuilder.withArgName("username").hasArg().isRequired(false)
            .withDescription("Login user name, by default -u ADMIN").create("u");

    @SuppressWarnings("static-access")
    private static final Option OPT_PWD = OptionBuilder.withArgName("password").hasArg().isRequired(false)
            .withDescription("Login password, by default -p KYLIN").create("p");

    private static final Options OPTIONS = new Options();
    {
        OPTIONS.addOption(OPT_SERVICE);
        OPTIONS.addOption(OPT_PROJECT);
        OPTIONS.addOption(OPT_USER);
        OPTIONS.addOption(OPT_PWD);
    }

    public static void main(String[] args) {
        QueryCLI cli = new QueryCLI(args, System.in, System.out);
        int exitCode = cli.run();
        System.exit(exitCode);
    }

    // ============================================================================

    final PrintWriter out;
    final BufferedReader in;

    ParseException parserException;
    String inpService;
    String inpProject;
    String inpUser;
    String inpPwd;
    String[] inpSqls;

    public QueryCLI(String[] args, InputStream in, OutputStream out) {
        try {
            this.out = new PrintWriter(out, true);
            this.in = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happens
        }

        try {
            GnuParser parser = new GnuParser();
            CommandLine line = parser.parse(OPTIONS, args);
            this.inpService = line.getOptionValue("s");
            this.inpProject = line.getOptionValue("project");
            this.inpUser = line.getOptionValue("u", "ADMIN");
            this.inpPwd = line.getOptionValue("p", "KYLIN");
            this.inpSqls = line.getArgs();
        } catch (ParseException e) {
            parserException = e;
        }
    }

    public int run() {
        if (parserException != null) {
            printHelp();
            return -1;
        }

        try {
            initDriver();
        } catch (Exception e) {
            out.println("[ERROR] Failed to init Driver" + e.getMessage());
            e.printStackTrace(out);
            return -2;
        }

        boolean hasError = false;
        for (String sqlOrFile : inpSqls) {
            try {

                runSql(sqlOrFile);

            } catch (Exception ex) {
                out.println("[ERROR] Failed to run: " + sqlOrFile);
                ex.printStackTrace(out);
                hasError = true;
            }

            // spacing between queries
            out.println();
        }

        return hasError ? -3 : 0;
    }

    private void initDriver() {
        try {
            Class.forName("org.apache.kylin.jdbc.Driver");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void runSql(String sqlOrFile) throws Exception {
        String sql = getSql(sqlOrFile);
        out.println("Running: " + sql);

        Properties props = new Properties();
        props.put("user", inpUser);
        props.put("password", inpPwd);
        Connection conn = DriverManager.getConnection("jdbc:kylin://" + inpService + "/" + inpProject, props);
        Statement stat = null;
        ResultSet rs = null;

        try {
            stat = conn.createStatement();

            long start;
            int placeholderCount = 0;
            if ((placeholderCount = KylinCheckSQL.countDynamicPlaceholder(sql)) > 0) {
                // prepared statement
                PreparedStatement prep = conn.prepareStatement(sql);
                stat = prep; // to be closed finally
                inputParams(prep, placeholderCount);
                start = System.currentTimeMillis();
                rs = prep.executeQuery();
            } else {
                // normal statement
                start = System.currentTimeMillis();
                rs = stat.executeQuery(sql);
            }

            int rows = printResultSet(rs);

            double seconds = (System.currentTimeMillis() - start) / (double) 1000;
            out.println("----");
            out.println(rows + " rows, " + seconds + " seconds");

        } finally {
            close(rs, stat, conn);
        }
    }

    private void inputParams(PreparedStatement prep, int placeholderCount) throws IOException, SQLException {
        if (placeholderCount > 0) {
            out.println("Input parameters E.g.:\n" + "\tb\'56\' for tinyint\n" + "\ts\'389\'for smallint\n"
                    + "\ti\'355\' for int\n" + "\tL\'260\' for long\n" + "\tf\'2.56\' for float\n"
                    + "\tdo\'2.556\'for double\n " + "\tda\'2018-01-01\' for date\n"
                    + "\tts\'2018-01-01 00:00:00\' for timestamp");
        }
        for (int i = 1; i <= placeholderCount; i++) {
            out.println("Input parameter no." + i + ": ");
            String input = in.readLine();
            Object obj = convertObject(input);
            prep.setObject(i, obj);

            out.println("Got parameter no." + i + ": " + obj + " (" + obj.getClass().getName() + ")");
        }
    }

    private Object convertObject(String input) {
        input = input.trim();

        String type;
        int cut1 = input.indexOf('\'');
        int cut2 = input.lastIndexOf('\'');
        if (cut1 < cut2) {
            type = input.substring(0, cut1).trim();
            input = input.substring(cut1 + 1, cut2);
        } else {
            type = guessType(input);
        }

        return convertToType(input, type);
    }

    private String guessType(String input) {
        try {
            Integer.parseInt(input);
            return "i";
        } catch (NumberFormatException e) {
            // ignore
        }
        try {
            Float.parseFloat(input);
            return "f";
        } catch (NumberFormatException e) {
            // ignore
        }
        try {
            java.sql.Date.valueOf(input);
            return "da";
        } catch (IllegalArgumentException e) {
            // ignore
        }
        try {
            java.sql.Timestamp.valueOf(input);
            return "ts";
        } catch (IllegalArgumentException e) {
            // ignore
        }
        return null;
    }

    private Object convertToType(String input, String type) {
        if (type == null || type.isEmpty())
            return input;
        switch (type) {
            case "b":
                return Byte.parseByte(input);
            case "s":
                return Short.parseShort(input);
            case "i":
                return Integer.parseInt(input);
            case "L":
                return Long.parseLong(input);
            case "f":
                return Float.parseFloat(input);
            case "do":
                return Double.parseDouble(input);
            case "da":
                return java.sql.Date.valueOf(input);
            case "ts":
                return java.sql.Timestamp.valueOf(input);
            default:
                return input;
        }
    }

    private String getSql(String sqlOrFile) throws IOException {
        File f = new File(sqlOrFile);
        if (f.exists())
            return FileUtils.readFileToString(f, "UTF-8");
        else
            return sqlOrFile;
    }

    private int printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int nCols = meta.getColumnCount();
        int nRows = 0;

        for (int i = 1; i <= nCols; i++) {
            if (i > 1)
                out.print("\t");
            out.print(meta.getColumnName(i));
        }
        out.println();

        while (rs.next()) {
            for (int i = 1; i <= nCols; i++) {
                if (i > 1)
                    out.print("\t");
                out.print(rs.getString(i));
            }
            out.println();
            nRows++;
        }

        return nRows;
    }

    private void close(ResultSet rs, Statement stat, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (stat != null) {
            try {
                stat.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(out, 100, "java org.apache.kylin.jdbc.QueryCLI [options] <sql-or-file>", "options:",
                OPTIONS, 2, 2, "");
    }

}
