package org.apache.kylin.storage.filter;

import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.coprocessor.DateConditionInplaceModifier;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Hongbin Ma(Binmahone) on 5/7/15.
 */
public class DateConditionInplaceModifierTest extends FilterBaseTest {
    @Test
    public void basicTest() {
        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc c1 = ColumnDesc.mockup(t1, 2, "CAL_DT", "date");
        TblColRef column = new TblColRef(c1);

        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter);
        ConstantTupleFilter constantFilter = null;
        constantFilter = new ConstantTupleFilter("946684800000");
        compareFilter.addChild(constantFilter);

        DateConditionInplaceModifier.modify(compareFilter);
        Assert.assertEquals("2000-01-01",compareFilter.getFirstValue());
    }
}
