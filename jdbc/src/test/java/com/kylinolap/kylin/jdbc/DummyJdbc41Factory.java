package com.kylinolap.kylin.jdbc;

import com.kylinolap.kylin.jdbc.stub.RemoteClient;

/**
 * @author xduo
 *
 */
public class DummyJdbc41Factory extends KylinJdbc41Factory {

	// ~ kylin sepcified
	@Override
	public RemoteClient newRemoteClient(KylinConnectionImpl connection) {
		return new DummyClient(connection);
	}

}
