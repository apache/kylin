package org.apache.kylin.metadata.sourceusage.mail;

import org.apache.kylin.common.constant.Constants;
import org.apache.kylin.common.mail.MailNotificationType;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.metadata.sourceusage.SourceUsageRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SourceUsageMailUtilTest extends NLocalFileMetadataTestCase {

    private static final String MAIL_TITLE_LOAD_EMPTY_DATA = "[Kylin System Notification]-[Over License Capacity Threshold]";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        overwriteSystemProp(Constants.KE_LICENSE_VOLUME, "");
        overwriteSystemProp("kylin.env", "DEV");
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testSourceUsageOverCapacityNotify() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
            SourceUsageRecord sourceUsageRecord = new SourceUsageRecord();
            sourceUsageManager.updateSourceUsage(sourceUsageRecord);

            sourceUsageRecord.setCurrentCapacity(10000L);
            sourceUsageRecord.setLicenseCapacity(10L);
            sourceUsageManager.updateSourceUsage(sourceUsageRecord);

            overwriteSystemProp("kylin.capacity.notification-enabled", "true");
            sourceUsageManager.updateSourceUsage(sourceUsageRecord);
            Assert.assertTrue(sourceUsageRecord.isCapacityNotification());

            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @Test
    public void testCreateMail() {
        Pair<String, String> mail = SourceUsageMailUtil.createMail(MailNotificationType.OVER_LICENSE_CAPACITY_THRESHOLD,
                100L, 101L);
        Assert.assertEquals(MAIL_TITLE_LOAD_EMPTY_DATA, mail.getFirst());
        Assert.assertTrue(mail.getSecond().contains("100"));
        Assert.assertTrue(mail.getSecond().contains("101"));
    }
}
