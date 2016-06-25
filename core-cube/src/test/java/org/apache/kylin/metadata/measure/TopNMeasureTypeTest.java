package org.apache.kylin.metadata.measure;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by shishaofeng on 6/6/16.
 */
public class TopNMeasureTypeTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() {
        this.createTestMetadata();

    }

    @After
    public void clear() {
        this.cleanupTestMetadata();
    }

    @Test
    public void test() {

        CubeDesc desc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_without_slr_left_join_desc");

        MeasureDesc topSellerMeasure = null;

        for (MeasureDesc measureDesc : desc.getMeasures()) {
            if (measureDesc.getName().equals("TOP_SELLER")) {
                topSellerMeasure = measureDesc;
                break;
            }
        }
        TopNMeasureType measureType = (TopNMeasureType) MeasureTypeFactory.create(topSellerMeasure.getFunction().getExpression(), topSellerMeasure.getFunction().getReturnDataType());

        topSellerMeasure.getFunction().getConfiguration().clear();
        List<TblColRef> colsNeedDict = measureType.getColumnsNeedDictionary(topSellerMeasure.getFunction());

        assertTrue(colsNeedDict != null && colsNeedDict.size() == 1);

        TblColRef sellerColRef = topSellerMeasure.getFunction().getParameter().getColRefs().get(1);
        topSellerMeasure.getFunction().getConfiguration().put(TopNMeasureType.CONFIG_ENCODING_PREFIX + sellerColRef.getName(), "int:6");
        colsNeedDict = measureType.getColumnsNeedDictionary(topSellerMeasure.getFunction());

        assertTrue(colsNeedDict.size() == 0);
    }
}
