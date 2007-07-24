package org.ajmm.obsearch.index;


import java.io.File;

import junit.framework.TestCase;

import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.index.utils.Directory;
import org.apache.log4j.Logger;


public class TestPPTree extends TestCase{

	private static transient final Logger logger = Logger
    .getLogger(TestPPTree.class);



	public void testPPTree() throws Exception{
		File dbFolder = new File(TUtils.getTestProperties().getProperty("test.db.path") );
    	Directory.deleteDirectory(dbFolder);
   	 	assertTrue(! dbFolder.exists());
   	 	assertTrue(dbFolder.mkdirs());
    	IndexShort<OBSlice> index = new PPTreeShort<OBSlice>(
                dbFolder, (byte) 30, (byte) 2, (short)0, (short) 200);

    	IndexSmokeTUtil t = new IndexSmokeTUtil();
    	t.tIndex(index);
    }

}
