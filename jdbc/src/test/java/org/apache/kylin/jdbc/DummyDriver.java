package org.apache.kylin.jdbc;

/**
 * @author xduo
 * 
 */
public class DummyDriver extends Driver {

    @Override
    protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        return "com.kylinolap.jdbc.DummyJdbc41Factory";
    }

}
