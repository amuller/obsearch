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
import net.obsearch.ob.OBShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class TestFrameworkShort<O extends OBShort> {

	private static transient final Logger logger = Logger
			.getLogger(TestFrameworkShort.class);

	private O[] queries;
	private O[] data;

	protected IndexShort<O> index;
	
	private Class<O> type;

	/**
	 * Create a new test with a DB size of dbSize and a query size of querySize.
	 * 
	 * @param dbSize
	 * @param querySize
	 * @param index
	 *            The index that will be used for testing.
	 */
	public TestFrameworkShort(Class<O> type, int dbSize, int querySize, IndexShort<O> index) {
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
	}
	
	protected void search() throws Exception{
		search(index, (short) 3, (byte) 3);
		search(index, (short) 7 , (byte) 3);       
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
            assertTrue("Status is: " + ex.getStatus() , ex.getStatus() == Status.OK);
            assertEquals((long)i, ex.getId());
            ex = index.exists(x);            
            assertTrue( "Exists after delete" + ex.getStatus() + " i " + i, ex.getStatus() == Status.NOT_EXISTS);
            i++;
        }
        index.close();
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
        
        index.resetStats();
        // it is time to Search
        
        String re = null;
        logger.info("Matching begins...");
        
        int i = 0;
        long realIndex = index.databaseSize();
        List < OBPriorityQueueShort < O >> result = new LinkedList < OBPriorityQueueShort < O >>();        
        while (i < this.queries.length) {
                OBPriorityQueueShort < O > x = new OBPriorityQueueShort < O >(
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

        Iterator < OBPriorityQueueShort < O >> it = result.iterator();
        i = 0;
        while (i < queries.length) {
        	if (i % 300 == 0) {
                    logger.info("Validating " + i + " of " + maxQuery);
        	}
                O s = queries[i];
                    OBPriorityQueueShort < O > x2 = new OBPriorityQueueShort < O >(
                            k);
                    searchSequential( s, x2, index, range);
                    OBPriorityQueueShort < O > x1 = it.next();
                    
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
private String debug( OBPriorityQueueShort < O > q, IndexShort<O> index) throws IllegalIdException, OBException, InstantiationException, IllegalAccessException{
	StringBuilder res = new StringBuilder();
	Iterator<OBResultShort<O>> it = q.iterator();
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
    public void searchSequential( O o,
            OBPriorityQueueShort < O > result,
            IndexShort < O > index, short range) throws Exception {
        int i = 0;
        while (i < data.length) {
            O obj = data[i];
            short res = o.distance(obj);
            if (res <= range) {
                result.add(i, obj, res);
            }
            i++;
        }
    }

	private void init() throws AlreadyFrozenException, IllegalIdException,
			OutOfRangeException, IOException, IllegalAccessException,
			InstantiationException, OBException {
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
