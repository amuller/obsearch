package net.obsearch.index.utils;

import hep.aida.bin.StaticBin1D;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBStorageException;
import net.obsearch.index.IndexShort;
import net.obsearch.ob.OBShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;

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

	public void search(IndexShort<O> index, short range, byte k)
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
		while (re != null) {
			String line = parseLine(re);
			if (line != null) {
				if (i % 300 == 0) {
					logger.info("Matching " + i + " of " + maxQuery);
				}
				O s = factory.create(line);
				if (factory.shouldProcess(s)) {
					OBPriorityQueueShort<O> x2 = new OBPriorityQueueShort<O>((int)index.databaseSize());
					searchSequential(realIndex, s, x2, index, range);
					OBPriorityQueueShort<O> x1 = it.next();
					// assertEquals("Error in query line: " + i + " slice: "
					// + line, x2, x1);

					stats.add(ep(x1,x2,index));

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
		logger.info("Finished  EP calculation: ");
		logger.info(StatsUtil.prettyPrintStats("EP", stats));
		assertFalse(it.hasNext());
	}
	
	private double ep(OBPriorityQueueShort<O> x1, OBPriorityQueueShort<O> x2, IndexShort < O > index) throws OBStorageException{
		List<OBResultShort<O>> query = x1.getSortedElements();
		List<OBResultShort<O>> db = x2.getSortedElements();
		int i = 0;
		int result = 0;
		Set<OBResultShort<O>> s = new HashSet<OBResultShort<O>>();
		for(OBResultShort<O> r : query){
			// find the position in the db. 
			int cx = 0;
			for(OBResultShort<O> c : db){
				if(s.contains(c)){
					cx++;
					continue;
				}
				if(c.compareTo(r) == 0){
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
			double res = ((double)result)/ ((double)(query.size() * index.databaseSize()));
			return res;
		}
	}

}
