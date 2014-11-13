package com.kylinolap.jdbc;

import com.kylinolap.jdbc.KylinConnectionImpl;
import com.kylinolap.jdbc.KylinJdbc41Factory;
import com.kylinolap.jdbc.stub.RemoteClient;

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
