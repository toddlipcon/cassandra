package com.facebook.infrastructure.db;

import com.facebook.infrastructure.io.*;

import junit.framework.TestCase;


public class TestRowMutation extends TestCase {
    private static final String TAB="MyTable";
    private static final String KEY="MyKey";
    private static final String CF="MyCF";
    private static final String COL_A="MyCF:Column_A";
    private static final String COL_B="MyCF:Column_B";
    private static final String COL_C="MyCF:Column_C";

    public void testSerialization() throws Exception
    {
        RowMutation rm = new RowMutation(TAB, KEY);
        rm.add(COL_A, "a data".getBytes());
        rm.add(COL_B, "b data".getBytes());
        rm.add(COL_C, "c data".getBytes());

        DataOutputBuffer buf = new DataOutputBuffer();
        RowMutationSerializer ser = new RowMutationSerializer();
        ser.serialize(rm, buf);

        DataInputBuffer inbuf = new DataInputBuffer();
        inbuf.reset(buf.getData(), buf.getLength());
        RowMutation rm2 = ser.deserialize(inbuf);

        assertEquals(rm.table(), rm2.table());
        assertEquals(rm.key(), rm2.key());

        assertEquals(rm.getModifications().size(), rm2.getModifications().size());

        ColumnFamily cf1 = rm.getModifications().get(CF);
        ColumnFamily cf2 = rm2.getModifications().get(CF);
        assertEquals(cf1.size(), cf2.size());
    }
}