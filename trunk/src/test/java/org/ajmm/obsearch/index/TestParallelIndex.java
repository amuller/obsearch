package org.ajmm.obsearch.index;


import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;


public class TestParallelIndex extends TestCase{

	@Override
    @Before
	public void setUp() throws Exception {
	}
	@Test
	public void testParallelIndexPPTree() throws Exception{
		// TODO: enable parallel index in the future.
	/*	File dbFolder = new File(TUtils.getTestProperties().getProperty("test.db.path"));
    	IndexSmokeTUtil.deleteDB(dbFolder);
   	 	assertTrue(! dbFolder.exists());
   	 	assertTrue(dbFolder.mkdirs());
   	 	    	
    	IndexShort<OBSlice> index = new PPTreeShort<OBSlice>(
                dbFolder, (byte) 30, (byte) 2, (short)0, (short) 200);
    	ParallelIndexShort<OBSlice> pindex = new ParallelIndexShort<OBSlice>(index,2,3000);
    	IndexSmokeTUtil t = new IndexSmokeTUtil();
    	t.tIndex(pindex);*/
    }

}
