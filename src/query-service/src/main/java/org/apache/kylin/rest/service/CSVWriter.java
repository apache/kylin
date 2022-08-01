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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.JavaConverters;

public class CSVWriter {

    private static final Logger logger = LoggerFactory.getLogger("query");

    private static final String QUOTE_CHAR = "\"";
    private static final String END_OF_LINE_SYMBOLS = IOUtils.LINE_SEPARATOR_UNIX;

    public void writeData(FileStatus[] fileStatuses, OutputStream outputStream,
                          String columnNames, String separator, boolean includeHeaders) throws IOException {

        try (Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
            if (includeHeaders) {
                writer.write(columnNames.replace(",", separator));
                writer.flush();
            }
            for (FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.getPath().getName().startsWith("_")) {
                    if (fileStatus.getPath().getName().endsWith("parquet")) {
                        writeDataByParquet(fileStatus, writer, separator);
                    } else {
                        writeDataByCsv(fileStatus, writer, separator);
                    }
                }
            }

            writer.flush();
        }
    }

    public static void writeCsv(Iterator<List<Object>> rows, Writer writer, String separator) {
        rows.forEachRemaining(row -> {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < row.size(); i++) {
                Object cell = row.get(i);
                String column = cell == null ? "" : cell.toString();

                if (i > 0) {
                    builder.append(separator);
                }

                final String escapedCsv = encodeCell(column, separator);
                builder.append(escapedCsv);
            }
            builder.append(END_OF_LINE_SYMBOLS); // EOL
            try {
                writer.write(builder.toString());
            } catch (IOException e) {
                logger.error("Failed to download asyncQueryResult csvExcel by parquet", e);
            }
        });
    }

    private void writeDataByParquet(FileStatus fileStatus, Writer writer, String separator) {
        List<org.apache.spark.sql.Row> rowList = SparderEnv.getSparkSession().read()
                .parquet(fileStatus.getPath().toString()).collectAsList();
        writeCsv(rowList.stream().map(row -> JavaConverters.seqAsJavaList(row.toSeq())).iterator(), writer, separator);
    }

    // the encode logic is copied from org.supercsv.encoder.DefaultCsvEncoder.encode
    private static String encodeCell(String cell, String separator) {

        boolean needQuote = cell.contains(separator) || cell.contains("\r") || cell.contains("\n");

        if (cell.contains(QUOTE_CHAR)) {
            needQuote = true;
            // escape
            cell = cell.replace(QUOTE_CHAR, QUOTE_CHAR + QUOTE_CHAR);
        }

        if (needQuote) {
            return QUOTE_CHAR + cell + QUOTE_CHAR;
        } else {
            return cell;
        }
    }

    private void writeDataByCsv(FileStatus fileStatus, Writer writer, String separator) {
        List<org.apache.spark.sql.Row> rowList = SparderEnv.getSparkSession().read()
                .csv(fileStatus.getPath().toString()).collectAsList();
        writeCsv(rowList.stream().map(row -> JavaConverters.seqAsJavaList(row.toSeq())).iterator(), writer, separator);
    }

}
