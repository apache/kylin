package org.apache.kylin.storage.filter;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

/**
 */
public class FilterPerfTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void foo() throws IOException, InterruptedException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        TableDesc tableDesc = MetadataManager.getInstance(kylinConfig).getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        TblColRef format = new TblColRef(tableDesc.findColumnByName("LSTG_FORMAT_NAME"));
        TblColRef categ = new TblColRef(tableDesc.findColumnByName("LEAF_CATEG_ID"));
        TblColRef site = new TblColRef(tableDesc.findColumnByName("LSTG_SITE_ID"));

        List<TupleFilter> ands = Lists.newArrayList();
        ands.add(buildOrFilterWithMultipleValues(format, Lists.newArrayList("Auction", "FP-GTC", "Others")));
        ands.add(buildOrFilterWithMultipleValues(categ, Lists.newArrayList("48027", "164261", "113802", "118687")));
        ands.add(buildOrFilterWithMultipleValues(site, Lists.newArrayList("0", "15", "3")));
        TupleFilter filter = buildAndFilter(ands);

        TupleInfo info = new TupleInfo();
        ColumnDesc[] columns = tableDesc.getColumns();
        for (int i = 0; i < columns.length; i++) {
            ColumnDesc column = columns[i];
            info.setField(column.getName(), new TblColRef(column), column.getDatatype(), i);
        }

        List<String> lines = Files.readLines(new File("../examples/test_case_data/localmeta/data/DEFAULT.TEST_KYLIN_FACT.csv"), Charset.defaultCharset());
        List<Tuple> tuples = Lists.newArrayList();

        for (String line : lines) {
            String[] tokens = line.split(",");
            if (tokens.length != columns.length) {
                System.out.println("invalid line");
                continue;
            }
            Tuple t = new Tuple(info);
            for (int k = 0; k < columns.length; k++) {
                ColumnDesc column = columns[k];
                t.setFieldObjectValue(column.getName(), tokens[k]);
            }
            tuples.add(t);
        }

        for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            System.out.println(i);
        }

        Iterator<Tuple> itr = Iterables.cycle(tuples).iterator();
        int TOTAL_LOOP = 1000000;
        int loopCount = 0;
        int matchCount = 0;
        long startTime = System.currentTimeMillis();
        while (itr.hasNext()) {
            if (filter.evaluate(itr.next())) {
                matchCount++;
            }

            if (++loopCount > TOTAL_LOOP) {
                break;
            }
        }
        System.out.println("Total match count: " + matchCount);
        System.out.println("ellapsed time: " + (System.currentTimeMillis() - startTime));
    }

    private TupleFilter buildOrFilterWithMultipleValues(TblColRef column, List<String> values) {
        List<TupleFilter> ors = Lists.newArrayList();
        for (String v : values) {
            ors.add(buildCompareFilter(column, v));
        }
        return buildOrFilter(ors);
    }

    protected CompareTupleFilter buildCompareFilter(TblColRef column, String v) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter);
        ConstantTupleFilter constantFilter = new ConstantTupleFilter(v);
        compareFilter.addChild(constantFilter);
        return compareFilter;
    }

    protected TupleFilter buildAndFilter(List<TupleFilter> ands) {
        LogicalTupleFilter andFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        for (TupleFilter x : ands) {
            andFilter.addChild(x);
        }
        return andFilter;
    }

    protected TupleFilter buildOrFilter(List<TupleFilter> ors) {
        LogicalTupleFilter orFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        for (TupleFilter x : ors) {
            orFilter.addChild(x);
        }
        return orFilter;
    }

}
