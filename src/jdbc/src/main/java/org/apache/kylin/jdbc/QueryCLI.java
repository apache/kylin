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
import java.nio.charset.StandardCharsets;
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

    private static final String PROJECT_ARG_NAME = "project";

    @SuppressWarnings("static-access")
    private static final Option OPT_SERVICE = OptionBuilder.withArgName("host:port").hasArg().isRequired()
            .withDescription("Service endpoint, e.g. -s localhost:7070").create("s");

    @SuppressWarnings("static-access")
    private static final Option OPT_PROJECT = OptionBuilder.withArgName(PROJECT_ARG_NAME).hasArg().isRequired()
            .withDescription("Project name, e.g. -project learn_kylin").create(PROJECT_ARG_NAME);

    @SuppressWarnings("static-access")
    private static final Option OPT_USER = OptionBuilder.withArgName("username").hasArg().isRequired(false)
            .withDescription("Login user name, by default -u ADMIN").create("u");

    @SuppressWarnings("static-access")
    private static final Option OPT_PWD = OptionBuilder.withArgName("password").hasArg().isRequired(false)
            .withDescription("Login password, by default -p KYLIN").create("p");

    private static final Options OPTIONS = new Options();
    static {
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
        this.out = new PrintWriter(out, true);
        this.in = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));

        try {
            GnuParser parser = new GnuParser();
            CommandLine line = parser.parse(OPTIONS, args);
            this.inpService = line.getOptionValue("s");
            this.inpProject = line.getOptionValue(PROJECT_ARG_NAME);
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
        int placeholderCount = KylinCheckSql.countDynamicPlaceholder(sql);
        boolean isPreparedStatement = placeholderCount > 0;
        ResultSet rs = null;
        try (Connection conn = DriverManager.getConnection("jdbc:kylin://" + inpService + "/" + inpProject, props)) {
            long start;
            if (isPreparedStatement) {
                try (PreparedStatement prep = conn.prepareStatement(sql)) {
                    start = System.currentTimeMillis();
                    rs = prep.executeQuery();
                }
            } else {
                try (Statement stat = conn.createStatement()) {
                    // normal statement
                    start = System.currentTimeMillis();
                    rs = stat.executeQuery(sql);
                }
            }
            int rows = printResultSet(rs);

            double seconds = (System.currentTimeMillis() - start) / (double) 1000;
            out.println("----");
            out.println(rows + " rows, " + seconds + " seconds");
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    private String getSql(String sqlOrFile) throws IOException {
        File f = new File(sqlOrFile);
        if (f.exists()) {
            return FileUtils.readFileToString(f, StandardCharsets.UTF_8);
        } else {
            return sqlOrFile;
        }
    }

    private int printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int nCols = meta.getColumnCount();
        int nRows = 0;

        for (int i = 1; i <= nCols; i++) {
            if (i > 1) {
                out.print("\t");
            }
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

    private void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(out, 100, "java org.apache.kylin.jdbc.QueryCLI [options] <sql-or-file>", "options:",
                OPTIONS, 2, 2, "");
    }

}
