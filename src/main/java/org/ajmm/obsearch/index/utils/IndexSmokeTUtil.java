package org.ajmm.obsearch.index.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.ajmm.obsearch.ParallelIndex;
import org.ajmm.obsearch.OperationStatus;
import org.ajmm.obsearch.Status;
import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.TimeStampResult;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.index.IndexShort;
import org.ajmm.obsearch.index.SynchronizableIndexShort;
import org.ajmm.obsearch.index.pivotselection.DummyPivotSelector;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;
import org.apache.log4j.Logger;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * Main class that performs all sorts of tests on the indexes. Objects are
 * inserted deleted verified for existence. Searches are always compared against
 * sequential search.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class IndexSmokeTUtil<O extends OBShort> {

    private OBFactory<O> factory;
    
    /**
     * Properties for the test.
     */
    Properties testProperties;

    /**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(IndexSmokeTUtil.class);
    
    

    /**
     * Creates a new smoke tester. Loads test properties.
     * @throws IOException
     *                 If the properties file cannot be found.
     */
    public IndexSmokeTUtil(OBFactory<O> factory) throws IOException {
        testProperties = TUtils.getTestProperties();
        this.factory =factory;
    }

    /**
     * Initialize the index.
     * @param index
     *                Index to be initialized.
     * @throws Exception
     *                 If something goes wrong.
     */
    public void initIndex(IndexShort < O > index) throws Exception {
        File query = new File(testProperties.getProperty("test.query.input"));
        File db = new File(testProperties.getProperty("test.db.input"));
        logger.debug("query file: " + query);
        logger.debug("db file: " + db);

        logger.info("Adding data");
        BufferedReader r = new BufferedReader(new FileReader(db));
        String re = r.readLine();
        int realIndex = 0;
        while (re != null) {
            String line = parseLine(re);
            if (line != null) {
                O s = factory.create(line);
                if (factory.shouldProcess(s)) {
                    OperationStatus res = index.insert(s);
                    assertTrue(
                            "Returned status: " + res.getStatus().toString(),
                            res.getStatus() == Status.OK);
                    assertEquals(realIndex, res.getId());
                    // If we insert before freezing, we should
                    // be getting a Result.EXISTS if we try to insert
                    // again!
                    assertTrue(!index.isFrozen());
                    res = index.insert(s);
                    assertTrue(res.getStatus() == Status.EXISTS);
                    assertEquals(res.getId(), realIndex);
                    realIndex++;
                }
            }
            re = r.readLine();

        }
        r.close();

        // "learn the data".
        logger.info("freezing");
        index.freeze();

        // we should test that the exists method works well
        r = new BufferedReader(new FileReader(db));
        re = r.readLine();

        logger.info("Checking exists and insert");
        int i = 0;
        while (re != null) {
            String line = parseLine(re);
            if (line != null) {
                O s = factory.create(line);
             
                if (factory.shouldProcess(s)) {
                    OperationStatus res = index.exists(s);
                    assertTrue("Str: " + line + " line: " + i, res.getStatus() == Status.EXISTS);
                    assertEquals(i, res.getId());
                    // attempt to insert the object again, and get
                    // the -1
                    res = index.insert(s);
                    assertEquals(res.getId(), i);
                    assertTrue(res.getStatus() == Status.EXISTS);
                    i++;
                }
                if (i % 10000 == 0) {
                    logger.info("Exists/insert : " + i);
                }

            }
            re = r.readLine();
        }

        assertEquals(realIndex, index.databaseSize());
        r.close();
    }

    /**
     * Test method for
     * {@link org.ajmm.obsearch.index.AbstractPivotIndex#insertObjectInDatabase(org.ajmm.obsearch.OB, int, com.sleepycat.je.Database)}.
     * Creates a database, fills it with data. Performs several queries and
     * compares the result with the sequential search.
     * @param index
     *                The index that will be tested
     * @exception If
     *                    something goes wrong.
     */
    public void tIndex(IndexShort < O > index) throws Exception {
        File query = new File(testProperties.getProperty("test.query.input"));
        File dbFolder = new File(testProperties.getProperty("test.db.path"));

        int cx = 0;

        initIndex(index);
        search(index, (short) 3, (byte) 3);
        search(index, (short) 10, (byte) 3);
        int i = 0;
        // int realIndex = 0;
        // test special methods that only apply to
        // SynchronizableIndex
        if (index instanceof SynchronizableIndex) {
            logger.info("Testing timestamp index");
            SynchronizableIndexShort < OBSlice > index2 = (SynchronizableIndexShort < OBSlice >) index;
            i = 0;
            int totalCx = 0;
            logger.info("Total Boxes: " + index2.totalBoxes());
            while (i < index2.totalBoxes()) {
                Iterator < TimeStampResult < OBSlice >> it2 = index2
                        .elementsNewerThan(i, 0);
                int cx2 = 0;
                while (it2.hasNext()) {
                    TimeStampResult < OBSlice > t = it2.next();
                    OBSlice o = t.getObject();
                    assert o != null;
                    // extract the object returned by the timestamp
                    // iterator and confirm that it is in the database.
                    OBPriorityQueueShort < OBSlice > x = new OBPriorityQueueShort < OBSlice >(
                            (byte) 1);
                    // it should be set to 0, but it won't work with 0.
                    index2.searchOB(o, (short) 5, x);
                    Iterator < OBResultShort < OBSlice >> it3 = x.iterator();
                    assertTrue(" Size found:" + x.getSize() + " item # " + cx
                            + " : " + o + " db size: " + index.databaseSize(),
                            x.getSize() == 1);
                    while (it3.hasNext()) {
                        OBResultShort < OBSlice > j = it3.next();
                        assertTrue(j.getObject().equals(o));
                        assertTrue(j.getObject().distance(o) == 0);
                    }
                    cx2++;

                }
                assertEquals(cx2, index2.elementsPerBox(i));
                logger.info("Result: box: " + i + " Cx" + cx2);
                totalCx += cx2;
                i++;
            }
            assertEquals(index.databaseSize(), totalCx);
            logger.info("CX: " + totalCx);
        }

        // now we delete elements from the DB
        logger.info("Testing deletes");
        i = 0;
        int max = index.databaseSize();
        while (i < max) {
            O x = index.getObject(i);
            OperationStatus ex = index.exists(x);
            assertTrue(ex.getStatus() == Status.EXISTS);
            assertTrue(ex.getId() == i);
            ex = index.delete(x);
            assertTrue(ex.getStatus() == Status.OK);
            assertEquals(i, ex.getId());
            ex = index.exists(x);
            assertTrue(ex.getStatus() == Status.NOT_EXISTS);
            i++;
        }
        index.close();
        Directory.deleteDirectory(dbFolder);
    }

    /**
     * Perform all the searches with
     * @param x
     *                the index that will be used
     * @param range
     * @param k
     */
    public void search(IndexShort < O > index, short range, byte k)
            throws Exception {
        // assertEquals(index.aDB.count(), index.bDB.count());
        // assertEquals(index.aDB.count(), index.bDB.count());
        // index.stats();
        // it is time to Search
        int querySize = 1642; // amount of elements to read from the query
        String re = null;
        logger.info("Matching begins...");
        File query = new File(testProperties.getProperty("test.query.input"));
        File dbFolder = new File(testProperties.getProperty("test.db.path"));
        BufferedReader r = new BufferedReader(new FileReader(query));
        List < OBPriorityQueueShort < O >> result = new LinkedList < OBPriorityQueueShort < O >>();
        re = r.readLine();
        int i = 0;
        int realIndex = index.databaseSize();

        while (re != null) {
            String line = parseLine(re);
            if (line != null) {
                OBPriorityQueueShort < O > x = new OBPriorityQueueShort < O >(
                        k);
                if (i % 100 == 0) {
                    logger.info("Matching " + i);
                }

                O s = factory.create(line);
                if (factory.shouldProcess(s)) {
                    index.searchOB(s, range, x);
                    result.add(x);
                    i++;
                }
            }
            if (i == querySize) {
                logger.warn("Finishing test at i : " + i);
                break;
            }
            re = r.readLine();
        }
        if (index instanceof ParallelIndex) {
            logger.info("Waiting for Queries");
            ((ParallelIndex) index).waitQueries();
        }
        int maxQuery = i;
        // logger.info("Matching ends... Stats follow:");
        // index.stats();

        // now we compare the results we got with the sequential search
        Iterator < OBPriorityQueueShort < O >> it = result.iterator();
        r.close();
        r = new BufferedReader(new FileReader(query));
        re = r.readLine();
        i = 0;
        while (re != null) {
            String line = parseLine(re);
            if (line != null) {
                if (i % 300 == 0) {
                    logger.info("Matching " + i + " of " + maxQuery);
                }
                O s = factory.create(line);
                if (factory.shouldProcess(s)) {
                    OBPriorityQueueShort < O > x2 = new OBPriorityQueueShort < O >(
                            k);
                    searchSequential(realIndex, s, x2, index, range);
                    OBPriorityQueueShort < O > x1 = it.next();
                    assertEquals("Error in query line: " + i + " slice: "
                            + line, x2, x1);
                    try{
                    // test the other search method
                    OBPriorityQueueShort < O > x3 = new OBPriorityQueueShort < O >(
                            k);
                    int[] inter = index.intersectingBoxes(s, range);
                    index.searchOB(s, range, x3, inter);

                    assertEquals("Error in intersectingBoxes: " + i
                            + " slice: " + line, x2, x3);
                    int box = 0; // this is just an index :)
                    while (box < index.totalBoxes()) {
                        if (isIn(box, inter)) {
                            assertTrue(index.intersects(s, range, box));
                        } else {
                            assertFalse(index.intersects(s, range, box));
                        }
                        box++;
                    }
                    
                    }catch(UnsupportedOperationException e){ 
                        // some indexes do not support boxes
                    }

                    i++;
                }
                
            }
            if (i == querySize) {
                logger.warn("Finishing test at i : " + i);
                break;
            }
            re = r.readLine();
        }
        r.close();
        logger.info("Finished  matching validation.");
        assertFalse(it.hasNext());
    }

    /**
     * if x is in j.
     * @param x
     *                item to search.
     * @param j
     *                array to search.
     * @return true if x is in j.
     */
    public static boolean isIn(int x, int[] j) {
        for (int k : j) {
            if (k == x) {
                return true;
            }
        }
        return false;
    }

    /**
     * We only process slices of this size.
     * @param x
     *                Slice
     * @return true if the slice is within the size we want.
     * @throws Exception
     *                 If something goes wrong.
     */
    public static boolean shouldProcessSlice(OBSlice x) throws Exception {
        return x.size() <= maxSliceSize;
    }
    
    public static int maxSliceSize = 500;

    /**
     * Parse a line in the slices file.
     * @param line
     *                A line in the file
     * @return null if the line is a comment or a String if the line is a valid
     *         tree representation
     */
    public static String parseLine(String line) {
        if (line.startsWith("//") || "".equals(line.trim())
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

    /**
     * Sequential search.
     * @param max
     *                Search all the ids in the database until max
     * @param o
     *                The object to search
     * @param result
     *                The queue were the results are stored
     * @param index
     *                the index to search
     * @param range
     *                The range to employ
     * @throws Exception
     *                 If something goes really bad.
     */
    public  void searchSequential(int max, O o,
            OBPriorityQueueShort < O > result,
            IndexShort < O > index, short range) throws Exception {
        int i = 0;
        while (i < max) {
            O obj = index.getObject(i);
            short res = o.distance(obj);
            if (res <= range) {
                result.add(i, obj, res);
            }
            i++;
        }
    }

}
