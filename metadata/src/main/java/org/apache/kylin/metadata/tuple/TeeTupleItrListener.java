package org.apache.kylin.metadata.tuple;

import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 5/13/15.
 */
public interface TeeTupleItrListener {
    void notify(List<ITuple> duplicated,long createTime);
}
