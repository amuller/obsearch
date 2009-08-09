package net.obsearch.example.vectors;

import hep.aida.bin.StaticBin1D;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.obsearch.ambient.Ambient;
import net.obsearch.ambient.bdb.AmbientBDBDb;
import net.obsearch.ambient.bdb.AmbientBDBJe;
import net.obsearch.ambient.my.AmbientMy;
import net.obsearch.ambient.tc.AmbientTC;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.ghs.impl.Sketch64Long;

import net.obsearch.index.perm.impl.DistPermLong;
import net.obsearch.index.utils.Directory;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezLong;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.pivots.rf02.RF02PivotSelectorShort;
import net.obsearch.pivots.rf03.RF03PivotSelectorLong;
import net.obsearch.pivots.rf03.RF03PivotSelectorShort;
import net.obsearch.query.OBQueryLong;

import net.obsearch.result.OBPriorityQueueLong;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;

public class VectorsDemoPer extends VectorsDemo {
	
	
	public static void main(String args[]) throws FileNotFoundException, OBStorageException, NotFrozenException, IllegalAccessException, InstantiationException, OBException, IOException, PivotsUnavailableException {
		
		init();
		
		// Delete the directory of the index just in case.
		Directory.deleteDirectory(INDEX_FOLDER);
		
		
		// Create a pivot selection strategy for L1 distance
		 //IncrementalMullerRosaShort<L1> sel = new IncrementalMullerRosaShort<L1>(
	 	//			new AcceptAll<L1>(), 4000, 1000, (short) Short.MAX_VALUE);
		
		
		IncrementalBustosNavarroChavezLong<L1Long> sel = new IncrementalBustosNavarroChavezLong<L1Long>(new AcceptAll<L1Long>(), 400, 400);
		//sel.setDesiredDistortion(0.10);
		//sel.setDesiredSpread(.70);
		// make the bit set as short so that m objects can fit in the buckets.
	    DistPermLong<L1Long> index = new DistPermLong<L1Long>(L1Long.class, sel, 128, 0);
	    index.setExpectedEP(EP);
	    index.setSampleSize(100);
	    index.setKAlpha(ALPHA);
	    // select the ks that the user will call.
	    index.setMaxK(new int[]{1});	    
	    index.setFixedRecord(true);
    	index.setFixedRecord(VEC_SIZE*2);
		// Create the ambient that will store the index's data. (NOTE: folder name is hardcoded)
		//Ambient<L1, Sketch64Short<L1>> a =  new AmbientBDBDb<L1, Sketch64Short<L1>>( index, INDEX_FOLDER );
	    //Ambient<L1, Sketch64Short<L1>> a =  new AmbientMy<L1, Sketch64Short<L1>>( index, INDEX_FOLDER );
    	Ambient<L1Long, DistPermLong<L1Long>> a =  new AmbientTC<L1Long, DistPermLong<L1Long>>( index, INDEX_FOLDER );
		
		// Add some random objects to the index:	
		logger.info("Adding " + DB_SIZE + " objects...");
		int i = 0;		
		while(i < DB_SIZE){
			index.insert(generateLongVector());
			if(i % 100000 == 0){
				logger.info("Loading: " + i);
			}
			i++;
		}
		
		// prepare the index
		logger.info("Preparing the index...");
		a.freeze();
		logger.info("YAY! stats: " + index.getStats());
		// add the rest of the objects
		/*while(i < DB_SIZE){
			index.insert(generateVector());
			if(i % 100000 == 0){
				logger.info("Loading: " + i);
			}
			i++;
		}*/
		
		// now we can match some objects!		
		logger.info("Querying the index...");
		i = 0;
		index.resetStats(); // reset the stats counter
		long start = System.currentTimeMillis();
		List<OBPriorityQueueLong<L1Long>> queryResults = new ArrayList<OBPriorityQueueLong<L1Long>>(QUERY_SIZE);
		List<L1Long> queries = new ArrayList<L1Long>(QUERY_SIZE);
		while(i < QUERY_SIZE){
			L1Long q = 	generateLongVector();	
			// query the index with k=1			
			OBPriorityQueueLong<L1Long> queue = new OBPriorityQueueLong<L1Long>(1);			
			// perform a query with r=3000000 and k = 1 
			index.searchOB(q, Long.MAX_VALUE, queue);
			queryResults.add(queue);
			queries.add(q);
			
			i++;
		}
		// print the results of the set of queries. 
		long elapsed = System.currentTimeMillis() - start;
		logger.info("Time per query: " + elapsed / QUERY_SIZE + " millisec.");
		
		logger.info("Stats follow: (total distances / pivot vectors computed during the experiment)");
		logger.info(index.getStats().toString());

		
		logger.info("Doing EP validation");
		StaticBin1D ep = new StaticBin1D();
		

		Iterator<OBPriorityQueueLong<L1Long>> it1 = queryResults.iterator();
		Iterator<L1Long> it2 = queries.iterator();
		StaticBin1D seqTime = new StaticBin1D();
		i = 0;
		while(it1.hasNext()){
			OBPriorityQueueLong<L1Long> qu = it1.next();
			L1Long q = it2.next();
			long time = System.currentTimeMillis();
			long[] sortedList = index.fullMatchLite(q, false);
			long el = System.currentTimeMillis() - time;
			seqTime.add(el);
			logger.info("Elapsed: " + el + " "  + i);
			OBQueryLong<L1Long> queryObj = new OBQueryLong<L1Long	>(q, Long.MAX_VALUE, qu, null);
			ep.add(queryObj.ep(sortedList));
			i++;
		}
		
		logger.info(ep.toString());
		logger.info("Time per seq query: ");
		logger.info(seqTime.toString());
		
	}

}
