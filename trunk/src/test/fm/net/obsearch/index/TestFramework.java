<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="TestFramework${Type}.java" />
package net.obsearch.index;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.ob.OB${Type};
import net.obsearch.result.OBPriorityQueue${Type};
import net.obsearch.result.OBResult${Type};

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
/*
		OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
    Copyright (C) 2008 Arnoldo Jose Muller Molina

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
	*  TestFramework${Type} performs several tests on an index. 
	*  This generated file works on ${type}s.
  *  @author      Arnoldo Jose Muller Molina    
  */
<@gen_warning filename="TestFramework.java "/>
public abstract class TestFramework${Type}<O extends OB${Type}> {

	private static transient final Logger logger = Logger
			.getLogger(TestFramework${Type}.class);

	protected O[] queries;
	protected O[] data;

	protected Index${Type}<O> index;
	
	private Class<O> type;

	/**
	 * Create a new test with a DB size of dbSize and a query size of querySize.
	 * 
	 * @param dbSize
	 * @param querySize
	 * @param index
	 *            The index that will be used for testing.
	 */
	public TestFramework${Type}(Class<O> type, int dbSize, int querySize, Index${Type}<O> index) {
		this.type = type;
		queries = createArray(querySize);
		data = createArray(dbSize);		
		this.index = index;
	}

	/**
	 * Perform all the tests and DB creation.
	 * @throws Exception 
	 */
	public void test() throws Exception {		
		init();
		search();
		deletes();
		init2();
		search();
		deletes();
		init3();
		search();
		deletes();
		close();
	}

	<@gen_warning filename="TestFramework.java "/>
	protected void search() throws Exception{
		search(index, (${type}) 3, (byte) 3);
		search(index, (${type}) 7 , (byte) 3);       
	}
	
	protected void deletes() throws Exception{
		logger.info("Testing deletes");
        int i = 0;       
        while (i < data.length) {
            O x = data[i];
            OperationStatus ex = index.exists(x);
            assertTrue(ex.getStatus() == Status.EXISTS);
            assertTrue(ex.getId() == i);
            ex = index.delete(x);
            assertTrue("Status is: " + ex.getStatus() + " i: " + i , ex.getStatus() == Status.OK);
            assertEquals((long)i, ex.getId());
            ex = index.exists(x);            
            assertTrue( "Exists after delete" + ex.getStatus() + " i " + i, ex.getStatus() == Status.NOT_EXISTS);
						if(i % 300 == 0){
								logger.info("Deleting... " + i );
						}
            i++;
        }
        
	}

	protected void close()throws Exception{
			index.close();
	}
	
  	/**
     * Perform all the searches with
     * @param x
     *                the index that will be used
     * @param range
     * @param k
     */
    public void search(Index${Type} < O > index, ${type} range, byte k)
            throws Exception {
        //range = (${type})Math.min(${ClassType}.MAX_VALUE, range);
        index.resetStats();
        // it is time to Search
        
        String re = null;
        logger.info("Matching begins...");
        
        int i = 0;
        long realIndex = index.databaseSize();
        List < OBPriorityQueue${Type} < O >> result = new LinkedList < OBPriorityQueue${Type} < O >>();        
        while (i < this.queries.length) {
                OBPriorityQueue${Type} < O > x = new OBPriorityQueue${Type} < O >(
                        k);
                if (i % 100 == 0) {
                    logger.info("Matching " + i);
                }

                O s = queries[i];                
                    index.searchOB(s, range, x);
                    result.add(x);
                    i++;                                        
        }
        logger.info("Range: " + range + " k " + k + " " + index.getStats().toString());
       
        int maxQuery = i;

        Iterator < OBPriorityQueue${Type} < O >> it = result.iterator();
        i = 0;
        while (i < queries.length) {
        	if (i % 300 == 0) {
                    logger.info("Validating " + i + " of " + maxQuery);
        	}
                O s = queries[i];
                    OBPriorityQueue${Type} < O > x2 = new OBPriorityQueue${Type} < O >(
                            k);
                    searchSequential( s, x2, index, range);
                    OBPriorityQueue${Type} < O > x1 = it.next();
                    
                    assertEquals("Error in query line: " + i + " " + index.debug(s) + "\n"
                            + debug(x2,index ) + "\n" + debug(x1,index), x2, x1);
                    i++;
                
                
            }
                   
        logger.info("Finished  matching validation.");
        assertFalse(it.hasNext());
    }

/**
 * Prints debug info for the given priority queue.	
 * @return
 * @throws IllegalAccessException 
 * @throws InstantiationException 
 * @throws OBException 
 * @throws IllegalIdException 
 */
private String debug( OBPriorityQueue${Type} < O > q, Index${Type}<O> index) throws IllegalIdException, OBException, InstantiationException, IllegalAccessException{
	StringBuilder res = new StringBuilder();
	Iterator<OBResult${Type}<O>> it = q.iterator();
	while(it.hasNext()){
		OBResult${Type}<O> r = it.next();
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
     * Sequential search.   
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
    <@gen_warning filename="TestFramework.java "/>
    public void searchSequential( O o,
            OBPriorityQueue${Type} < O > result,
            Index${Type} < O > index, ${type} range) throws Exception {
        int i = 0;
        while (i < data.length) {
            O obj = data[i];
            ${type} res = o.distance(obj);
            if (res <= range) {
                result.add(i, obj, res);
            }
            i++;
        }
    }

	private void init() throws AlreadyFrozenException, IllegalIdException,
			OutOfRangeException, IOException, IllegalAccessException,
			InstantiationException, OBException, PivotsUnavailableException {
		// initialize data.
		initQueries();
		initData();
		// insert data into the index.
		logger.info("Inserting data...");
		int i = 0;
		while (i < data.length) {
			O s = data[i];
			if(i % 1000 == 0){
				logger.info("Inserting: " + i);
			}
			OperationStatus res = index.insert(s);
			assertTrue("Returned status: " + res.getStatus().toString(), res
					.getStatus() == Status.OK);
			assertEquals((long)i, res.getId());
			// If we insert before freezing, we should
			// be getting a Result.EXISTS if we try to insert
			// again!
			assertTrue(!index.isFrozen());
			res = index.insert(s);
			assertTrue(res.getStatus() == Status.EXISTS);
			assertEquals(res.getId(), (long)i);
			i++;

		}

		// "learn the data".
		logger.info("freezing");
		index.freeze();

		logger.info("Checking exists and insert");
		i = 0;
		while (i < data.length) {

			O s = data[i];
			OperationStatus res = index.exists(s);
			assertTrue("Str: " + s.toString() + " line: " + i,
					res.getStatus() == Status.EXISTS);
			assertEquals((long)i, res.getId());
			// attempt to insert the object again, and get
			// the -1
			res = index.insert(s);
			assertEquals(res.getId(), (long)i);
			assertTrue(res.getStatus() == Status.EXISTS);
			i++;

			if (i % 10000 == 0) {
				logger.info("Exists/insert : " + i);
			}

		}
		assertEquals((long)i, index.databaseSize());

	}



	/**
   * Test insert(O,long) insertions.
   */
	private void init2() throws AlreadyFrozenException, IllegalIdException,
			OutOfRangeException, IOException, IllegalAccessException,
			InstantiationException, OBException {

			//index.setIdAutoGeneration(false);

		// insert data into the index.
		logger.info("Inserting data...");
		int i = 0;
		while (i < data.length) {
			O s = data[i];
			if(i % 1000 == 0){
				logger.info("Inserting: " + i);
			}
			OperationStatus res = index.insert(s, i);
			assertTrue("Returned status: " + res.getStatus().toString(), res
					.getStatus() == Status.OK);
			assertEquals((long)i, res.getId());

			res = index.insert(s,i);
			assertTrue(res.getStatus() == Status.EXISTS);
			assertEquals(res.getId(), (long)i);
			i++;

		}


		logger.info("Checking exists and insert");
		i = 0;
		while (i < data.length) {

			O s = data[i];
			OperationStatus res = index.exists(s);
			assertTrue("Str: " + s.toString() + " line: " + i,
					res.getStatus() == Status.EXISTS);
			assertEquals((long)i, res.getId());
			// attempt to insert the object again, and get
			// the -1
			res = index.insert(s,i);
			assertEquals(res.getId(), (long)i);
			assertTrue(res.getStatus() == Status.EXISTS);
			i++;

			if (i % 10000 == 0) {
				logger.info("Exists/insert : " + i);
			}

		}
		assertEquals((long)i, index.databaseSize());

	}


	private void init3() throws AlreadyFrozenException, IllegalIdException,
			OutOfRangeException, IOException, IllegalAccessException,
			InstantiationException, OBException {

			//index.setIdAutoGeneration(false);

		// insert data into the index.
		logger.info("Inserting data (bulk)...");
		int i = 0;
		while (i < data.length) {
			O s = data[i];
			if(i % 1000 == 0){
				logger.info("Inserting: " + i);
			}
			OperationStatus res = index.insertBulk(s, i);
			assertTrue("Returned status: " + res.getStatus().toString(), res
					.getStatus() == Status.OK);
			assertEquals((long)i, res.getId());
		
			i++;

		}


		logger.info("Checking exists and insert");
		i = 0;
		while (i < data.length) {

			O s = data[i];
			OperationStatus res = index.exists(s);
			assertTrue("Str: " + s.toString() + " line: " + i,
					res.getStatus() == Status.EXISTS);
			assertEquals((long)i, res.getId());			

			if (i % 10000 == 0) {
				logger.info("Exists/insert : " + i);
			}
			i++;
		}
		assertEquals((long)i, index.databaseSize());

	}

	private void initQueries() {
		int i = 0;
		while (i < queries.length) {
			queries[i] = nextQuery();
			i++;
		}
	}

	private void initData() {
		int i = 0;
		while (i < data.length) {
			data[i] = next();
			i++;
		}
	}

	/**
	 * Create an array of size size.
	 * 
	 * @param size
	 * @return
	 */
	protected O[] createArray(int size){
		return (O[])Array.newInstance(type, size);
	}

	/**
	 * Create a new object.
	 * 
	 * @return a new O object.
	 */
	protected abstract O next();

	/**
	 * Generate an object for the query, defaults {@link #nextO()}.
	 * 
	 * @return {@link #nextO()}.
	 */
	protected O nextQuery() {
		return next();
	}
}
</#list>