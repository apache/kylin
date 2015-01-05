/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.dict.lookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

import com.kylinolap.metadata.tool.HiveClient;

public class HiveTableReader implements TableReader {

    private String dbName;
    private String tableName;
    private int currentSplit = -1;
    private ReaderContext readCntxt;
    private Iterator<HCatRecord> currentHCatRecordItr = null;
    private HCatRecord currentHCatRecord;
    private int numberOfSplits = 0;

    public HiveTableReader(String dbName, String tableName) throws IOException {
        this.dbName = dbName;
        this.tableName = tableName;
        try {
            HiveClient hiveClient = new HiveClient();
            this.readCntxt = hiveClient.getReaderContext(dbName, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }

        this.numberOfSplits = readCntxt.numSplits();
    }

    @Override
    public void close() throws IOException {
        this.readCntxt = null;
        this.currentHCatRecordItr = null;
        this.currentHCatRecord = null;
        this.currentSplit = -1;
    }

    @Override
    public boolean next() throws IOException {
        while (currentHCatRecordItr == null || !currentHCatRecordItr.hasNext()) {
            currentSplit++;
            if (currentSplit == numberOfSplits) {
                return false;
            }
            currentHCatRecordItr = loadHCatRecordItr(currentSplit);
        }

        currentHCatRecord = currentHCatRecordItr.next();

        return true;
    }

    private Iterator<HCatRecord> loadHCatRecordItr(int dataSplit) throws HCatException {
        HCatReader currentHCatReader = DataTransferFactory.getHCatReader(readCntxt, dataSplit);
        return currentHCatReader.read();
    }

    @Override
    public String[] getRow() {
        List<Object> allFields = currentHCatRecord.getAll();
        List<String> rowValues = new ArrayList<String>(allFields.size());
        for (Object o : allFields) {
            rowValues.add(o != null ? o.toString() : "NULL");
        }

        return rowValues.toArray(new String[allFields.size()]);
    }

    @Override
    public void setExpectedColumnNumber(int expectedColumnNumber) {

    }

    public String toString() {
        return "hive table reader for: " + dbName + "." + tableName;
    }

}
