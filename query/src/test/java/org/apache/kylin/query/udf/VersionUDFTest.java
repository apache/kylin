package org.apache.kylin.query.udf;

import static org.junit.Assert.assertTrue;

import org.apache.kylin.common.KylinVersion;
import org.junit.Test;

public class VersionUDFTest {
    @Test
    public void testVersionUDF() {
        String currentVer = KylinVersion.getCurrentVersion().toString();
        String udfVer = new VersionUDF().eval();
        assertTrue(currentVer.equals(udfVer));
    }
}
