package net.obsearch.index.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


import net.obsearch.Index;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.example.OBSlice;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;

import net.obsearch.index.IndexShort;
import net.obsearch.ob.OBShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;
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

    protected OBFactory<O> factory;
    
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
    	index.setPreFreezeCheck(true);
        File query = new File(testProperties.getProperty("test.query.input"));
        File db = new File(testProperties.getProperty("test.db.input"));
        logger.debug("query file: " + query);
        logger.debug("db file: " + db);        
        logger.info("Adding data");
        BufferedReader r = new BufferedReader(new FileReader(db));
        String re = r.readLine();
        long realIndex = 0;
        long i = 0;
        final int totalToLoad = 10000;
        while (re != null && i < totalToLoad) {
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
                    i++;
                }
            }
            re = r.readLine();

        }
        r.close();

        // "learn the data".
        logger.info("freezing");
        index.freeze();
        logger.info(index.getStats());
        

        
        // we should test that the exists method works well
        r = new BufferedReader(new FileReader(db));
        re = r.readLine();

        logger.info("Checking exists and insert");
        i = 0;
        while (re != null) {
            String line = parseLine(re);
            if (line != null) {
                O s = factory.create(line);
             
                if(i >= totalToLoad){
                	break; 
                }
                
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
        //search(index, (short) 3, (byte) 3);
        //search(index, (short) 7, (byte) 1);
        //search(index, (short) 12, (byte) 3);
        
        search(index, (short) 1000, (byte) 1);

        search(index, (short) 1000, (byte) 3);

        search(index, (short) 1000, (byte) 10);
        
        search(index, (short) 1000, (byte) 50);
        
        long i = 0;
        // int realIndex = 0;
        // test special methods that only apply to
        // SynchronizableIndex
       

        // now we delete elements from the DB
        logger.info("Testing deletes");
        i = 0;
        long max = index.databaseSize();
        while (i < max) {
            O x = index.getObject(i);
            OperationStatus ex = index.exists(x);
            assertTrue(ex.getStatus() == Status.EXISTS);
            assertTrue(ex.getId() == i);
            ex = index.delete(x);
            assertTrue("Status is: " + ex.getStatus() + " i: " + i , ex.getStatus() == Status.OK);
            assertEquals(i, ex.getId());
            ex = index.exists(x);            
            assertTrue( "Exists after delete" + ex.getStatus() + " i " + i, ex.getStatus() == Status.NOT_EXISTS);
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
    public void search(IndexShort < O > index, short range, short k)
            throws Exception {
        // assertEquals(index.aDB.count(), index.bDB.count());
        // assertEquals(index.aDB.count(), index.bDB.count());
        // index.stats();
        index.resetStats();
        // it is time to Search
        int querySize = 1000; // amount of elements to read from the query
        String re = null;
        logger.info("Matching begins...");
        File query = new File(testProperties.getProperty("test.query.input"));
        File dbFolder = new File(testProperties.getProperty("test.db.path"));
        BufferedReader r = new BufferedReader(new FileReader(query));
        List < OBPriorityQueueShort < O >> result = new LinkedList < OBPriorityQueueShort < O >>();
        re = r.readLine();
        int i = 0;
        long realIndex = index.databaseSize();

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
                    if(i == 279){
                        System.out.println("hey");
                    }
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
        
        logger.info(index.getStats().toString());
       
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
                    //assertEquals("Error in query line: " + i + " slice: "
                    //        + line, x2, x1);  
                    
                    assertEquals("Error in query line: " + i + " " + index.debug(s) + "\n slice: "
                            + line + " " + debug(x2,index ) + "\n" + debug(x1,index), x2, x1);

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
    
    protected String debug(OBPriorityQueueShort < O > q , Index<O> index) throws IllegalIdException, OBException, InstantiationException, IllegalAccessException{
    	return debug(q.iterator(), index);
    }
    /**
     * Prints debug info for the given priority queue.	
     * @return
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws OBException 
     * @throws IllegalIdException 
     */
    protected String debug(Iterator<OBResultShort<O>>it , Index<O> index) throws IllegalIdException, OBException, InstantiationException, IllegalAccessException{
    	StringBuilder res = new StringBuilder();
    	
    	while(it.hasNext()){
    		OBResultShort<O> r = it.next();
    		res.append(r.getId());
    		res.append(" r: ");
    		res.append(r.getDistance());
    		res.append("\n");
    		res.append(index.debug(index.getObject(r.getId())));
    		res.append("\n");
    	}
    	return res.toString();
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
    public  void searchSequential(long max, O o,
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
