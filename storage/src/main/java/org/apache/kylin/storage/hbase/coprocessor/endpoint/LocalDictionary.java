package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.IDictionaryAware;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorRowType;

import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 3/3/15.
 */
public class LocalDictionary implements IDictionaryAware {

    private CoprocessorRowType type;
    private List<Dictionary<?>> dicts;
    private TableRecordInfoDigest recordInfo;

    public LocalDictionary(List<Dictionary<?>> dicts, CoprocessorRowType type, TableRecordInfoDigest recordInfo) {
        this.dicts = dicts;
        this.type = type;
        this.recordInfo = recordInfo;
    }

    @Override
    public int getColumnLength(TblColRef col) {
        return recordInfo.length(type.getColIndexByTblColRef(col));
    }

    @Override
    public Dictionary<?> getDictionary(TblColRef col) {
        return this.dicts.get(type.getColIndexByTblColRef(col));
    }
}
