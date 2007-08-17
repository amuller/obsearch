/**
 *
 */
package org.ajmm.obsearch.index;

import java.io.File;
import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.index.utils.Directory;
import org.apache.log4j.Logger;
import junit.framework.TestCase;


/**
 * Perform different tests on the pyramid technique
 *
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class TestExtentedPyramidIndex extends TestCase {


    private static transient final Logger logger = Logger
            .getLogger(TestExtentedPyramidIndex.class);


    
    public void testPyramid() throws Exception{
    	// TODO: enable this test:
    	File dbFolder = new File(TUtils.getTestProperties().getProperty("test.db.path") );
    	Directory.deleteDirectory(dbFolder);
   	 	assertTrue(! dbFolder.exists());
       assertTrue(dbFolder.mkdirs());
    	IndexShort<OBSlice> index = new ExtendedPyramidIndexShort<OBSlice>(
                 dbFolder, (byte) 15, (short)0, (short) 200);
    	IndexSmokeTUtil t = new IndexSmokeTUtil();
    	t.tIndex(index);
    }


}
