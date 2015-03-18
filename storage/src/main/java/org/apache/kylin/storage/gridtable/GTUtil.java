package org.apache.kylin.storage.gridtable;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

public class GTUtil {

    static final TableDesc MOCKUP_TABLE = new TableDesc();
    static {
        MOCKUP_TABLE.setName("GT_MOCKUP_TABLE");
    }

    public static TblColRef tblColRef(int col, String datatype) {
        ColumnDesc desc = new ColumnDesc();
        String id = "" + (col + 1);
        desc.setId(id);
        desc.setName(id);
        desc.setDatatype(datatype);
        desc.init(MOCKUP_TABLE);
        return new TblColRef(desc);
    }
}
