package org.apache.kylin.dict;

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable;

/**
 * To build dictionary, we need a list of distinct values on a column.
 * For column on lookup table, simply scan the whole table since the table is small.
 * For column on fact table, the fact table is too big to iterate. So the build
 * engine will first extract distinct values (by a MR job for example), and
 * implement this interface to provide the result to DictionaryManager.
 */
public interface DistinctColumnValuesProvider {

    /** Return a ReadableTable contains only one column, each row being a distinct value. */
    public ReadableTable getDistinctValuesFor(TblColRef col);
}
