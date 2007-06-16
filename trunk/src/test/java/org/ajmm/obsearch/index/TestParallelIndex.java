package org.ajmm.obsearch.index;


import java.io.File;

import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.testutils.OBSlice;
import org.junit.Before;
import org.junit.Test;

public class TestParallelIndex {

	@Before
	public void setUp() throws Exception {
	}
	@Test
	public void testParallelIndexPPTree() throws Exception{
    	File dbFolder = new File(TUtils.getTestProperties().getProperty("test.db.path"));
    	IndexShort<OBSlice> index = new PPTreeShort<OBSlice>(
                dbFolder, (byte) 30, (byte) 8, (short)0, (short) 200);
    	ParallelIndexShort pindex = new ParallelIndexShort(index,4,3000);
    	IndexSmokeTUtil t = new IndexSmokeTUtil();
    	t.tIndex(pindex);
    }

}
