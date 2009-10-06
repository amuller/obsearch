package net.obsearch.index.utils;

import hep.aida.bin.StaticBin1D;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import net.obsearch.AbstractOBResult;
import net.obsearch.ApproxIndexShort;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.index.IndexShort;
import net.obsearch.ob.OBShort;
import net.obsearch.query.OBQueryShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;
import net.obsearch.stats.Statistics;

import org.apache.log4j.Logger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexSmokeTUtilApprox<O extends OBShort> extends IndexSmokeTUtil<O> {
	
	public IndexSmokeTUtilApprox(OBFactory<O> factory) throws IOException {
		super(factory);
	}
	
	/**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(IndexSmokeTUtilApprox.class);

	public void search(IndexShort<O> index, short range, short k)
			throws Exception {
		// assertEquals(index.aDB.count(), index.bDB.count());
		// assertEquals(index.aDB.count(), index.bDB.count());
		// index.stats();
		OBAsserts.chkAssert(index.databaseSize() <= Integer.MAX_VALUE, "Cannot test with more than 2^32 items");
		index.resetStats();
		// it is time to Search
		int querySize = 1000; // amount of elements to read from the query
		String re = null;
		logger.info("Matching begins...");
		File query = new File(testProperties.getProperty("test.query.input"));
		File dbFolder = new File(testProperties.getProperty("test.db.path"));
		BufferedReader r = new BufferedReader(new FileReader(query));
		List<OBPriorityQueueShort<O>> result = new LinkedList<OBPriorityQueueShort<O>>();
		re = r.readLine();
		int i = 0;
		long realIndex = index.databaseSize();

		while (re != null) {
			String line = parseLine(re);
			if (line != null) {
				OBPriorityQueueShort<O> x = new OBPriorityQueueShort<O>(k);
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

		logger.info(index.getStats().toString());

		int maxQuery = i;
		// logger.info("Matching ends... Stats follow:");
		// index.stats();

		// now we compare the results we got with the sequential search
		Iterator<OBPriorityQueueShort<O>> it = result.iterator();
		r.close();
		r = new BufferedReader(new FileReader(query));
		re = r.readLine();
		i = 0;
		StaticBin1D stats = new StaticBin1D();
		int emptyResults = 0;
		int badResults = 0;
		int zeroResults = 0;
		IntegerHolder badCount = new IntegerHolder(0);
		Statistics st = new Statistics();
		StaticBin1D recall = new StaticBin1D();
		while (re != null) {
			String line = parseLine(re);
			if (line != null) {
				if (i % 300 == 0) {
					logger.info("Matching " + i + " of " + maxQuery);
				}
				O s = factory.create(line);
				if (factory.shouldProcess(s)) {
					
					ArrayList<OBResultShort<O>> x2 = new ArrayList<OBResultShort<O>>((int)index.databaseSize());

					searchSequential(realIndex, s, x2, index, range);
					OBPriorityQueueShort<O> x1 = it.next();
					// assertEquals("Error in query line: " + i + " slice: "
					// + line, x2, x1);
					st.addExtraStats("ReturnedSize", x2.size());
					if(isApproxZero(x1,x2,range)){
						//logger.info("Error in query line: " + i + " " + index.debug(s) + "\n slice: "
	                    //        + line + " " + debug(x2.getSortedElements().subList(0, Math.min(3, x2.getSize())).iterator(),index ) + "\n");
						emptyResults++;
					}else{
						double ep = ep(x1,x2,index);
						if(ok(x1,x2,range)){
							assertTrue(ep == 0);
						}
						stats.add(ep);
						if(index instanceof ApproxIndexShort){
							OBQueryShort<O> q = new OBQueryShort<O>(null, x1);
							List<AbstractOBResult<O>> seq = new LinkedList<AbstractOBResult<O>>();
							for(OBResultShort<O> o : x2){
								seq.add(o);
							}
							double anotherEp = q.ep(seq);
							assertTrue("index ep " + ep + " priority ep: " + anotherEp, ep == anotherEp);
							
						}
					}
					if(!ok(x1,x2, range)){
						badResults++;
					}
					
					if(zeroResults(x2,range)){
						zeroResults++;
					}
					recall.add(recall(x1,x2,k,range));
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
		logger.info("Finished  CompoundError calculation: ");
		logger.info("Returned size" + st);
		logger.info(StatsUtil.prettyPrintStats("CompoundError", stats));
		assertFalse(it.hasNext());
		logger.info("Recall: " + recall.toString());
		logger.info("Zero queries: " + emptyResults);
		logger.info("Bad results: " + badResults);
		logger.info("Zero (real) queries : " + zeroResults);
	}
	
	/**
	 * 
	 * @param db database
	 * @param range range query
	 * @return true if the results are zero.
	 */
	public boolean zeroResults( List<OBResultShort<O>> db, short range){
		return db.get(0).getDistance() > range;
	}
	
	/**
	 * If x1 (query) returned zero results but db had results, this function returns
	 * true. Otherwise false.
	 * @param x1
	 * @param db
	 * @param range
	 * @return
	 */
	public boolean isApproxZero(OBPriorityQueueShort<O> x1, List<OBResultShort<O>> db, short range){
		return x1.getSize() == 0 && (db.size() != 0 && db.get(0).getDistance() <= range);
	}
	
	public boolean ok(OBPriorityQueueShort<O> x1, List<OBResultShort<O>> db, short range){
		
		// if the db is empty, then we don't have to worry about anything else.
		if(zeroResults(db,range)){
			assert x1.getSize() == 0;
			return true;
		}
		
		List<OBResultShort<O>> query = x1.getSortedElements();
		
		if(query.size() == 0 && db.size() != 0){
			return false;
		}
		assert query.size() <= db.size() : "Query: " + query + " db: " + db;
		Iterator<OBResultShort<O>> it = db.iterator();
		for(OBResultShort<O> r : query){
			assert it.hasNext();
			if(it.hasNext() && r.compareTo(it.next()) != 0){
				return false;
			}
		}
		return true;
	}
	
	public double recall(OBPriorityQueueShort<O> x1, List<OBResultShort<O>> db, int k, short range){
		if(zeroResults(db,range)){
			assert x1.getSize() == 0;
			return 1;
		}
		int hits = 0;
		Set<OBResultShort<O>> s = new HashSet<OBResultShort<O>>();
		for(OBResultShort<O> r : x1.getSortedElements()){			
			int i = 0;
			for(OBResultShort<O> d : db){
				if(i == k){
					break;
				}
				if(! s.contains(d) &&d.compareTo(r) == 0){
					s.add(d);
					hits++;
					break;
				}
				i++;
			}
		}
		return (double)hits / (double)k;
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
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
     * @throws Exception
     *                 If something goes really bad.
     */
    public  void searchSequential(long max, O o,
            ArrayList < OBResultShort<O> > result,
            IndexShort < O > index, short range) throws OBException, IllegalAccessException, InstantiationException {
        int i = 0;
        while (i < max) {
            O obj = index.getObject(i);
            short res = o.distance(obj);
            //if (res <= range) {
                result.add( new OBResultShort<O>(obj, i, res));
            //}
            i++;
        }
        Collections.sort(result);
        Collections.reverse(result);
    }
	
	public double ep(OBPriorityQueueShort<O> x1, List<OBResultShort<O>> db, IndexShort < O > index) throws OBStorageException{
		List<OBResultShort<O>> query = x1.getSortedElements();
		int i = 0;
		int result = 0;
		Set<OBResultShort<O>> s = new HashSet<OBResultShort<O>>();
		for(OBResultShort<O> r : query){
			// find the position in the db. 
			int cx = 0;
			for(OBResultShort<O> c : db){
				if(! s.contains(c) &&c.compareTo(r) == 0){
					s.add(c);
					result += cx - i;
					break;
				}
				cx++;
			}
			i++;
		}
		if(query.size() == 0){
			return 0;
		}else{
			double res = ((double)result)/ ((double)(query.size() * db.size()));
			return res;
		}
	}

}
