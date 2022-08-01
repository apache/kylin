package org.apache.kylin.query.runtime;

import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.query.engine.exec.sparder.QueryEngine;

public class MockEngine implements QueryEngine {

    @Override
    public List<List<String>> compute(DataContext dataContext, RelNode relNode) {
        return null;
    }
}
