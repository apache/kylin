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

package org.apache.kylin.storage.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

/**
 * @author yangli9
 * 
 */
public class RegionScannerAdapter implements RegionScanner {

    private ResultScanner scanner;

    public RegionScannerAdapter(ResultScanner scanner) {
        this.scanner = scanner;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        Result result = scanner.next();
        if (result == null) // EOF
            return false;

        results.addAll(result.listCells());
        return true;
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return next(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }

    @Override
    public int getBatch() {
        return -1;
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    @Override
    public HRegionInfo getRegionInfo() {
        return null;
    }

    @Override
    public long getMaxResultSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isFilterDone() throws IOException {
        return false;
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMvccReadPoint() {
        return Long.MAX_VALUE;
    }

}
