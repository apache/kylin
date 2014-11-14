package com.kylinolap.storage.filter;

import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;

public class BitMapFilterEvaluatorTest {

    static TblColRef colA;
    
    static {
        TableDesc table = new TableDesc();
        table.setName("TABLE");
        ColumnDesc col = new ColumnDesc();
        col.setTable(table);
        col.setName("colA");
        colA = new TblColRef(col);
    }
    
    
}
