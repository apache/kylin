package org.apache.kylin.jdbc;

/**
 * @author xduo
 * 
 */
public class DummyDriver extends Driver {

    @Override
    protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        return "org.apache.kylin.jdbc.DummyJdbc41Factory";
    }

}
