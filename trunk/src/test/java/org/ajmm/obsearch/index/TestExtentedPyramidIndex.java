/**
 * 
 */
package org.ajmm.obsearch.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.ajmm.obsearch.AbstractOBPriorityQueue;
import org.ajmm.obsearch.AbstractOBResult;
import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.index.pivotselection.DummyPivotSelector;
import org.ajmm.obsearch.index.pivotselection.RandomPivotSelector;
import org.ajmm.obsearch.index.pivotselection.TentaclePivotSelectorShort;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.testutils.OBSlice;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;

import junit.framework.TestCase;

/*
 * OBSearch: a distributed similarity search engine This project is to
 * similarity search what 'bit-torrent' is to downloads. Copyright (C) 2007
 * Arnoldo Jose Muller Molina
 * 
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
/**
 * Perform different tests on the pyramid technique
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class TestExtentedPyramidIndex extends TestCase {

    Properties testProperties;

    private static transient final Logger logger = Logger
            .getLogger(TestExtentedPyramidIndex.class);

    protected void init() throws IOException {

    }

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#setUp()
     */
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        testProperties = TUtils.getTestProperties();
        PropertyConfigurator.configure(testProperties
                .getProperty("test.log4j.file"));
    }

    /**
     * Test method for
     * {@link org.ajmm.obsearch.index.AbstractPivotIndex#insertObjectInDatabase(org.ajmm.obsearch.OB, int, com.sleepycat.je.Database)}.
     * Creates a database, fills it with a bunch of rows performs a query and
     * compares the result with the sequential search.
     */
    public void testFull() throws Exception {
        File dbFolder = new File(testProperties.getProperty("test.db.path"));
        assertTrue(dbFolder.mkdirs());
        try {
            File query = new File(testProperties
                    .getProperty("test.query.input"));
            File db = new File(testProperties.getProperty("test.db.input"));

            ExtendedPyramidIndexShort<OBSlice> index = new ExtendedPyramidIndexShort<OBSlice>(
                    dbFolder, (byte) 15, (short)0, (short) 10000);
            logger.info("Adding data");
            BufferedReader r = new BufferedReader(new FileReader(db));
            String re = r.readLine();
            int realIndex = 0;
            while (re != null) {
                String line = parseLine(re);
                if (line != null) {
                	OBSlice s = new OBSlice(line);
                	if(this.shouldProcessSlice(s)){
                		index.insert(s, realIndex);
                		realIndex++;
                	}
                }
                re = r.readLine();
                
            }

            // select the pivots
            //TentaclePivotSelectorShort<OBSlice> ps = new TentaclePivotSelectorShort<OBSlice>((short)5);
            RandomPivotSelector ps = new RandomPivotSelector();
            ps.generatePivots(index);
            // the pyramid values are created
            logger.info("freezing");
            index.freeze();

            assertEquals(index.aDB.count(), index.bDB.count());
            assertEquals(index.aDB.count(), index.bDB.count());
            //index.stats();
            byte k = 3;
            short range = 3; // range
            // it is time to Search
            logger.info("Pyramid matching begins...");
            r = new BufferedReader(new FileReader(query));
            List<OBPriorityQueueShort<OBSlice>> result = 
				new LinkedList<OBPriorityQueueShort<OBSlice>>();
            re = r.readLine();
            int i = 0;
            
			while (re != null) {
                String line = parseLine(re);
                if (line != null) {
                	OBPriorityQueueShort<OBSlice> x = new OBPriorityQueueShort<OBSlice>(k);
                    if (i % 300 == 0) {
                        logger.info("Matching " + i);
                    }
                    OBSlice s = new OBSlice(line);
                	if(this.shouldProcessSlice(s)){
                		index.searchOB(s, range, x);
                    	result.add(x);
                    	i++;     
                	}
                }
                if(i == 1642){
                    logger.warn("Finishing test at i : " + i);
                    break;
                }
                re = r.readLine();
            }
            int maxQuery = i;
            logger.info("Pyramid matching ends... Stats follow:");
            //index.stats();
            // now we compare the results we got with the real thing!
            Iterator<OBPriorityQueueShort<OBSlice>> it = result.iterator();
            r = new BufferedReader(new FileReader(query));
            re = r.readLine();
            i = 0;
            while (re != null) {
                String line = parseLine(re);
                if (line != null) {
                    if (i % 300 == 0) {
                        logger.info("Matching " + i + " of " + maxQuery);
                    }
                    OBSlice s = new OBSlice(line);
                	if(this.shouldProcessSlice(s)){
                		OBPriorityQueueShort<OBSlice> x2 = new OBPriorityQueueShort<OBSlice>(k);
                    	searchSequential(realIndex, s, x2, index,
                            range);
                    	OBPriorityQueueShort<OBSlice> x1 = it.next();                   
                    	assertEquals("Error in query line: " + i, x2, x1);
                    	i++;
                	}
                }
                if(i == 1642){
                    logger.warn("Finishing test at i : " + i);
                    break;
                }
                re = r.readLine();
            }
            logger.info("Finished pyramid matching...");
            assertFalse(it.hasNext());
        } finally {
            // we should delete the databases no matter what happens
            File[] files = dbFolder.listFiles();
            for (File f : files) {
                assertTrue(f.delete());
            }
            assertTrue(dbFolder.delete());
        }
    }
    
    public boolean shouldProcessSlice(OBSlice x) throws Exception{
    	return x.size()<= 100;
    }

    /**
     * 
     * @param max
     * @param o
     * @param result
     * @param index
     * @param range
     * @throws Exception
     */
    public void searchSequential(int max, OBSlice o,
    		OBPriorityQueueShort<OBSlice> result,
            ExtendedPyramidIndexShort<OBSlice> index, short range)
            throws Exception {
        int i = 0;
        while (i < max) {
            OBSlice obj = index.getObject(i);
            short res = o.distance(obj);
            if (res <= range) {
                result.add(i, obj, res);
            }
            i++;
        }
    }

    public String parseLine(String line) {
        if (line.startsWith("//") || line.trim().equals("")
                || (line.startsWith("#") && !line.startsWith("#("))) {
            return null;
        } else {
            String arr[] = line.split("[:]");
            if (arr.length == 2) {
                return arr[1];
            } else if (arr.length == 1) {
                return arr[0];
            } else {
                assert false : "Received line: " + line;
                return null;
            }
        }
    }

}
