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

package org.apache.kylin.storage.hbase.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A simple test case to check whether the Hive JDBC interface can fulfill Kylin's need; 
 * Before run it, you need startup the hive server on localhost: $HIVE_HOME/bin/hiveserver2
 * @author shaoshi
 *
 */
public class HiveJDBCClientTest {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private static String tempFileName = "/tmp/a.txt";

    //@Before
    public void setup() {

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        File testFile = new File(tempFileName);

        if (!testFile.exists()) {

            FileWriter writer;
            try {
                writer = new FileWriter(testFile);
                writer.write("1 a\n");
                writer.write("2 b\n");

                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    //@After
    public void tearDown() {

    }

    protected Connection getHiveConnection() throws SQLException {

        return DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
    }

    //@Test
    public void testConnectToHive() throws SQLException {

        //replace "hive" here with the name of the user the queries should run as
        Connection con = getHiveConnection();

        Statement stmt = con.createStatement();
        String tableName = "testHiveDriverTable";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string) row format delimited fields terminated by ' '");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        res.close();

        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
        res.close();

        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
        String filepath = tempFileName;
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }
        res.close();

        // regular hive query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getInt(1));
        }
        res.close();

        stmt.close();
        con.close();
    }

    //@Test
    public void testShowExtendedTable() throws SQLException {
        Connection con = getHiveConnection();

        String tableName = "testHiveDriverTable";
        String sql = "show table extended like " + tableName + "";

        ResultSet res = executeSQL(con, sql);
        while (res.next()) {
            System.out.println("--- " + res.getString("tab_name"));
        }

        sql = "describe extended " + tableName + "";
        res = executeSQL(con, sql);
        while (res.next()) {
            System.out.println("---" + res.getString(1) + " | " + res.getString(2));
        }

        res.close();
        con.close();
    }

    protected ResultSet executeSQL(Connection con, String sql) throws SQLException {

        Statement stmt = con.createStatement();
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        return stmt.getResultSet();
    }

    public void runTests() throws SQLException {
        try {
            setup();
            testConnectToHive();
            testShowExtendedTable();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            tearDown();
        }

    }

    public static void main(String[] args) throws SQLException {
        HiveJDBCClientTest test = new HiveJDBCClientTest();
        test.runTests();
    }

}
