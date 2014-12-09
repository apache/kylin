package com.kylinolap.metadata.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by qianzhou on 12/3/14.
 */
public class TableDescTest {

    @Test
    public void testTableIdentity() {
        final String tableName = "testtable";
        final String dbName = "testdb";
        TableDesc table = new TableDesc();
        table.setName(tableName);
        assertEquals(("DEFAULT." + tableName).toUpperCase(), TableDesc.getTableIdentity(table));
        assertEquals(("DEFAULT." + tableName).toUpperCase(), TableDesc.getTableIdentity(tableName));
        assertEquals(("DEFAULT." + tableName).toUpperCase(), TableDesc.getTableIdentity("DEFAULT." + tableName));
        assertEquals((dbName + "." + tableName).toUpperCase(), TableDesc.getTableIdentity(dbName + "." + tableName));

        table.setDatabase(dbName);
        assertEquals((dbName + "." + tableName).toUpperCase(), TableDesc.getTableIdentity(dbName + "." + tableName));

        try {
            TableDesc.getTableIdentity("1 2");
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            System.out.println(ex.getMessage());
        }
        try {
            TableDesc.getTableIdentity("t.2.abc");
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            System.out.println(ex.getMessage());
        }

    }
}
