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

package org.apache.kylin.storage.hbase.cube.v1;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.google.common.collect.Lists;

/**
 * @author yangli9
 * 
 */
public class ResultScannerAdapter implements ResultScanner {

    private RegionScanner scanner;

    public ResultScannerAdapter(RegionScanner scanner) {
        this.scanner = scanner;
    }

    @Override
    public Iterator<Result> iterator() {
        return new Iterator<Result>() {

            Result next = null;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    try {
                        next = ResultScannerAdapter.this.next();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return next != null;
            }

            @Override
            public Result next() {
                Result r = next;
                next = null;
                return r;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Result next() throws IOException {
        List<Cell> cells = Lists.newArrayList();
        scanner.next(cells);
        if (cells.isEmpty())
            return null;
        else
            return Result.create(cells);
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            scanner.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
