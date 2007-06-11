package org.ajmm.obsearch.index;


import java.io.File;

import junit.framework.TestCase;

import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.testutils.OBSlice;
import org.apache.log4j.Logger;


public class TestPPTree extends TestCase{

	private static transient final Logger logger = Logger
    .getLogger(TestPPTree.class);



	public void testPPTree() throws Exception{
    	File dbFolder = new File(TUtils.getTestProperties().getProperty("test.db.path"));
    	IndexShort<OBSlice> index = new PPTreeShort<OBSlice>(
                dbFolder, (byte) 15, (byte) 2, (short)0, (short) 10000);

    	IndexSmokeTUtil t = new IndexSmokeTUtil();
    	t.tIndex(index);
    }

}
