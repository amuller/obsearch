package net.obsearch.example.vectors;

import hep.aida.bin.StaticBin1D;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.obsearch.ambient.Ambient;

import net.obsearch.ambient.bdb.AmbientBDBJe;

import net.obsearch.ambient.tc.AmbientTC;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.ghs.impl.Sketch64Float;
import net.obsearch.index.ghs.impl.Sketch64Long;

import net.obsearch.index.utils.Directory;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.pivots.rf02.RF02PivotSelectorShort;
import net.obsearch.pivots.rf03.RF03PivotSelectorLong;
import net.obsearch.pivots.rf03.RF03PivotSelectorShort;
import net.obsearch.pivots.rf04.RF04PivotSelectorFloat;
import net.obsearch.query.OBQueryFloat;
import net.obsearch.query.OBQueryLong;

import net.obsearch.result.OBPriorityQueueFloat;
import net.obsearch.result.OBPriorityQueueLong;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;

public class VectorsDemoGHS extends VectorsDemo {
	
	
	
	
	public static void main(String args[]) throws FileNotFoundException, OBStorageException, NotFrozenException, IllegalAccessException, InstantiationException, OBException, IOException, PivotsUnavailableException {
		
		init();
		
		// Delete the directory of the index just in case.
		Directory.deleteDirectory(INDEX_FOLDER);
		
		
		// Create the pivot selection strategy
		RF04PivotSelectorFloat<L1Float> sel = new RF04PivotSelectorFloat<L1Float>(new AcceptAll<L1Float>());
		sel.setDataSample(400);
		
		
		//sel.setDesiredDistortion(0.10);
		//sel.setDesiredSpread(.70);
		// make the bit set as short so that m objects can fit in the buckets.
		// create an index.
		// Choose pivot sizes that are multiples of 64 to optimize the space
	    Sketch64Float<L1Float> index = new Sketch64Float<L1Float>(L1Float.class, sel, 256, 0);
	    index.setExpectedEP(1.40);
	    index.setSampleSize(100);
	    index.setKAlpha(ALPHA);
	    // select the ks that the user will call.
	    index.setMaxK(new int[]{1});	    
	    index.setFixedRecord(true);
    	index.setFixedRecord(VEC_SIZE*4);
		// Create the ambient that will store the index's data. (NOTE: folder name is hardcoded)
		//Ambient<L1, Sketch64Short<L1>> a =  new AmbientBDBDb<L1, Sketch64Short<L1>>( index, INDEX_FOLDER );
	    //Ambient<L1, Sketch64Short<L1>> a =  new AmbientMy<L1, Sketch64Short<L1>>( index, INDEX_FOLDER );
    	Ambient<L1Float, Sketch64Float<L1Float>> a =  new AmbientTC<L1Float, Sketch64Float<L1Float>>( index, INDEX_FOLDER );
		
		// Add some random objects to the index:	
		logger.info("Adding " + DB_SIZE + " objects...");
		int i = 0;		
		while(i < DB_SIZE){
			index.insert(generateFloatVector());
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
		List<OBPriorityQueueFloat<L1Float>> queryResults = new ArrayList<OBPriorityQueueFloat<L1Float>>(QUERY_SIZE);
		List<L1Float> queries = new ArrayList<L1Float>(QUERY_SIZE);
		while(i < QUERY_SIZE){
			L1Float q = 	generateFloatVector();	
			// query the index with k=1			
			OBPriorityQueueFloat<L1Float> queue = new OBPriorityQueueFloat<L1Float>(1);			
			// perform a query with r=3000000 and k = 1 
			index.searchOB(q, Float.MAX_VALUE, queue);
			queryResults.add(queue);
			queries.add(q);
			
			i++;
		}
		// print the results of the set of queries. 
		long elapsed = System.currentTimeMillis() - start;
		logger.info("Time per query: " + elapsed / QUERY_SIZE + " millisec.");
		
		logger.info("Stats follow: (total distances / pivot vectors computed during the experiment)");
		logger.info(index.getStats().toString());

		
		logger.info("Doing CompoundError validation");
		StaticBin1D ep = new StaticBin1D();
		

		Iterator<OBPriorityQueueFloat<L1Float>> it1 = queryResults.iterator();
		Iterator<L1Float> it2 = queries.iterator();
		StaticBin1D seqTime = new StaticBin1D();
		i = 0;
		while(it1.hasNext()){
			OBPriorityQueueFloat<L1Float> qu = it1.next();
			L1Float q = it2.next();
			long time = System.currentTimeMillis();
			float[] sortedList = index.fullMatchLite(q, false);
			long el = System.currentTimeMillis() - time;
			seqTime.add(el);
			logger.info("Elapsed: " + el + " "  + i);
			OBQueryFloat<L1Float> queryObj = new OBQueryFloat<L1Float	>(q, Float.MAX_VALUE, qu, null);
			ep.add(queryObj.approx(sortedList));
			i++;
		}
		
		logger.info(ep.toString());
		logger.info("Time per seq query: ");
		logger.info(seqTime.toString());
		
	}

}
