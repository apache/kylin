package org.apache.kylin.metadata.tuple;

import static org.junit.Assert.*;

import org.junit.Test;

public class EmptyTupleIteratorTest {

    @Test
    public void testListAllTables() throws Exception {
        ITupleIterator it = ITupleIterator.EMPTY_TUPLE_ITERATOR;
        assertFalse(it.hasNext());
        assertNull(it.next());
        it.close(); // for coverage
    }

}
