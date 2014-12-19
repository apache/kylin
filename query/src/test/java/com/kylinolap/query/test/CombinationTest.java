package com.kylinolap.query.test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.kylinolap.storage.hbase.coprocessor.observer.ObserverEnabler;

/**
 * Created by honma on 7/2/14.
 */
@RunWith(Parameterized.class)
@Ignore
public class CombinationTest extends KylinQueryTest {

    @BeforeClass
    public static void setUp() throws SQLException {
    }

    @AfterClass
    public static void tearDown() {
        clean();
    }

    /**
     * return all config combinations, where first setting specifies join type
     * (inner or left), and the second setting specifies whether to force using
     * coprocessors(on, off or unset).
     */
    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { "inner", "unset" }, { "left", "unset" }, { "inner", "off" }, { "left", "off" }, { "inner", "on" }, { "left", "on" }, });
    }

    public CombinationTest(String joinType, String coprocessorToggle) throws Exception {

        KylinQueryTest.clean();

        KylinQueryTest.joinType = joinType;
        KylinQueryTest.setupAll();
        KylinQueryTest.preferCubeOf(joinType);

        if (coprocessorToggle.equals("on")) {
            ObserverEnabler.forceCoprocessorOn();
        } else if (coprocessorToggle.equals("off")) {
            ObserverEnabler.forceCoprocessorOff();
        } else if (coprocessorToggle.equals("unset")) {
            // unset
        }
    }
}
