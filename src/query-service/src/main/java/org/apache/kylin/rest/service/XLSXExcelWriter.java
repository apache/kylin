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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;

import lombok.val;

public class XLSXExcelWriter {

    private static final Logger logger = LoggerFactory.getLogger("query");

    public void writeData(FileStatus[] fileStatuses, Sheet sheet) {
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.getPath().getName().startsWith("_")) {
                if (fileStatus.getPath().getName().endsWith("parquet")) {
                    writeDataByParquet(fileStatus, sheet);
                } else if (fileStatus.getPath().getName().endsWith("xlsx")) {
                    writeDataByXlsx(fileStatus, sheet);
                } else {
                    writeDataByCsv(fileStatus, sheet);
                }
            }
        }
    }

    private void writeDataByXlsx(FileStatus f, Sheet sheet) {
        boolean createTempFileStatus = false;
        File file = new File("temp.xlsx");
        try {
            createTempFileStatus = file.createNewFile();
            FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
            fileSystem.copyToLocalFile(f.getPath(), new Path(file.getPath()));
        } catch (Exception e) {
            logger.error("Export excel writeDataByXlsx create exception f:{} createTempFileStatus:{} ",
                    f.getPath(), createTempFileStatus, e);
        }
        try (InputStream is = new FileInputStream(file.getAbsolutePath());
             XSSFWorkbook sheets = new XSSFWorkbook(is)) {
            final AtomicInteger offset = new AtomicInteger(sheet.getPhysicalNumberOfRows());
            XSSFSheet sheetAt = sheets.getSheetAt(0);
            for (int i = 0; i < sheetAt.getPhysicalNumberOfRows(); i++) {
                XSSFRow row = sheetAt.getRow(i);
                org.apache.poi.ss.usermodel.Row excelRow = sheet.createRow(offset.get());
                offset.incrementAndGet();
                for (int index = 0; index < row.getPhysicalNumberOfCells(); index++) {
                    XSSFCell cell = row.getCell(index);
                    excelRow.createCell(index).setCellValue(getString(cell));
                }
            }
            Files.delete(file.toPath());
        } catch (Exception e) {
            logger.error("Export excel writeDataByXlsx handler exception f:{} createTempFileStatus:{} ",
                    f.getPath(), createTempFileStatus, e);
        }
    }

    private static String getString(XSSFCell xssfCell) {
        if (xssfCell == null) {
            return "";
        }
        if (xssfCell.getCellType() == CellType.NUMERIC) {
            return String.valueOf(xssfCell.getNumericCellValue());
        } else if (xssfCell.getCellType() == CellType.BOOLEAN) {
            return String.valueOf(xssfCell.getBooleanCellValue());
        } else {
            return xssfCell.getStringCellValue();
        }
    }

    private void writeDataByParquet(FileStatus fileStatus, Sheet sheet) {
        final AtomicInteger offset = new AtomicInteger(sheet.getPhysicalNumberOfRows());
        List<org.apache.spark.sql.Row> rowList = SparderEnv.getSparkSession().read()
                .parquet(fileStatus.getPath().toString()).collectAsList();
        rowList.stream().forEach(row -> {
            org.apache.poi.ss.usermodel.Row excelRow = sheet.createRow(offset.get());
            offset.incrementAndGet();
            val list = row.toSeq().toList();
            for (int i = 0; i < list.size(); i++) {
                Object cell = list.apply(i);
                String column = cell == null ? "" : cell.toString();
                excelRow.createCell(i).setCellValue(column);
            }
        });
    }

    public void writeDataByCsv(FileStatus fileStatus, Sheet sheet) {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        List<String> rowResults = Lists.newArrayList();
        List<String[]> results = Lists.newArrayList();
        final AtomicInteger offset = new AtomicInteger(sheet.getPhysicalNumberOfRows());
        try (FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath())) {
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            rowResults.addAll(Lists.newArrayList(bufferedReader.lines().collect(Collectors.toList())));
            for (String row : rowResults) {
                results.add(row.split(SparderEnv.getSeparator()));
            }
            for (int i = 0; i < results.size(); i++) {
                Row row = sheet.createRow(offset.get());
                offset.incrementAndGet();
                String[] rowValues = results.get(i);
                for (int j = 0; j < rowValues.length; j++) {
                    row.createCell(j).setCellValue(rowValues[j]);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to download asyncQueryResult xlsxExcel by csv", e);
        }
    }
}
