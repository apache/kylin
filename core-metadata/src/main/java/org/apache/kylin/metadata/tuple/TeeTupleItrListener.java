package org.apache.kylin.metadata.tuple;

import java.util.List;

/**
 */
public interface TeeTupleItrListener {
    void notify(List<ITuple> duplicated,long createTime);
}
