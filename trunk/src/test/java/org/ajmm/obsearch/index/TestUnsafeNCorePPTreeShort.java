package org.ajmm.obsearch.index;


import java.io.File;

import junit.framework.TestCase;

import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.index.utils.Directory;
import org.junit.Before;

public class TestUnsafeNCorePPTreeShort extends TestCase{

    @Before
    public void setUp() throws Exception {
    }

    /**
     * Tests on the P+Tree.
     * @throws Exception If something goes really bad.
     */
    public void testPPTree() throws Exception {
        File dbFolder = new File(TUtils.getTestProperties().getProperty(
                "test.db.path"));
        Directory.deleteDirectory(dbFolder);
        assertTrue(!dbFolder.exists());
        assertTrue(dbFolder.mkdirs());
        IndexShort < OBSlice > index = new UnsafeNCorePPTreeShort < OBSlice >(dbFolder,
                (byte) 30, (byte) 8, (short) 0, (short) 200,2);

        IndexSmokeTUtil t = new IndexSmokeTUtil();
        t.tIndex(index);
    }
}
