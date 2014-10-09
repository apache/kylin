package com.kylinolap.kylin.jdbc;

/**
 * @author xduo
 *
 */
public class DummyDriver extends Driver {

	@Override
	protected String getFactoryClassName(JdbcVersion jdbcVersion) {
		return "com.kylinolap.kylin.jdbc.DummyJdbc41Factory";
	}

}
