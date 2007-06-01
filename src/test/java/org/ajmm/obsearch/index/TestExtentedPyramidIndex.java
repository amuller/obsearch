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

import org.ajmm.obsearch.OBPriorityQueue;
import org.ajmm.obsearch.OBResult;
import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.dimension.ShortDim;
import org.ajmm.obsearch.index.pivotselection.DummyPivotSelector;
import org.ajmm.obsearch.index.pivotselection.RandomPivotSelector;
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

            ExtendedPyramidIndex<OBSlice, ShortDim> index = new ExtendedPyramidIndex<OBSlice, ShortDim>(
                    dbFolder, (byte) 30, new ShortDim((short) 0), new ShortDim(
                            (short) 10000));
            logger.info("Adding data");
            BufferedReader r = new BufferedReader(new FileReader(db));
            String re = r.readLine();
            int realIndex = 0;
            while (re != null) {
                String line = parseLine(re);
                if (line != null) {
                    index.insert(new OBSlice(line), realIndex);
                }
                re = r.readLine();
                realIndex++;
            }

            // we select the pivots and put all the stuff
            // in the database
            // the pyramid values are created
            logger.info("freezing");
            index.freeze(new RandomPivotSelector());

            assertEquals(index.aDB.count(), index.bDB.count());
            assertEquals(index.aDB.count(), index.bDB.count());

            byte k = 3;
            ShortDim range = new ShortDim((short) 3); // range
            // it is time to Search
            logger.info("Pyramid matching begins...");
            r = new BufferedReader(new FileReader(query));
            List<OBPriorityQueue<OBSlice, ShortDim>> result = 
				new LinkedList<OBPriorityQueue<OBSlice, ShortDim>>();
            re = r.readLine();
            int i = 0;
            
			while (re != null) {
                String line = parseLine(re);
                if (line != null) {
                    OBPriorityQueue<OBSlice, ShortDim> x = new OBPriorityQueue<OBSlice, ShortDim>(
                            k);
                    if (i % 100 == 0) {
                        logger.info("Matching " + i);
                    }
                    index.searchOB(new OBSlice(line), range, x);
                    result.add(x);
                    i++;
                    if( i == 300){
                        break;
                    }
                }
                re = r.readLine();
            }
            int maxQuery = i;
            logger.info("Pyramid matching ends...");
            // now we compare the results we got with the real thing!
            Iterator<OBPriorityQueue<OBSlice, ShortDim>> it = result.iterator();
            r = new BufferedReader(new FileReader(query));
            re = r.readLine();
            i = 0;
            while (re != null) {
                String line = parseLine(re);
                if (line != null) {
                    if (i % 100 == 0) {
                        logger.info("Matching " + i + " of " + maxQuery);
                    }
                    OBPriorityQueue<OBSlice, ShortDim> x2 = new OBPriorityQueue<OBSlice, ShortDim>(
                            k);
                    searchSequential(realIndex, new OBSlice(line), x2, index,
                            range);
                    OBPriorityQueue<OBSlice, ShortDim> x1 = it.next();
                    logger.info(x2);
                    assertEquals("Error in query line: " + i, x2, x1);
                    i++;
                }

                re = r.readLine();
                if( i == 300){
                    break;
                }
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
            OBPriorityQueue<OBSlice, ShortDim> result,
            ExtendedPyramidIndex<OBSlice, ShortDim> index, ShortDim range)
            throws Exception {
        int i = 0;
        OBResult<OBSlice, ShortDim> partial = new OBResult<OBSlice, ShortDim>();
        while (i < max) {
            OBSlice obj = index.getObject(i);
            ShortDim res = new ShortDim((short) -1);
            obj.distance(o, res);
            if (res.le(range)) {
                partial.setDistance(res);
                partial.setId(i);
                partial.setObject(obj);
                result.add(partial);
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
