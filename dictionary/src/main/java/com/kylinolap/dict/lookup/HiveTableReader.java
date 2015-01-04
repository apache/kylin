package com.kylinolap.dict.lookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
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

    public HiveTableReader(String dbName, String tableName) throws MetaException, ClassNotFoundException, CommandNeedRetryException, IOException {
        this.dbName = dbName;
        this.tableName = tableName;
        this.readCntxt = HiveClient.getInstance().getReaderContext(dbName, tableName);
        this.numberOfSplits = readCntxt.numSplits();
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

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
