package net.obsearch.example.vectors;

import hep.aida.bin.StaticBin1D;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import net.obsearch.ambient.Ambient;
import net.obsearch.ambient.my.AmbientMy;
import net.obsearch.ambient.tc.AmbientTC;
import net.obsearch.example.AbstractExampleGeneral;
import net.obsearch.example.AbstractGHSExample;
import net.obsearch.example.doc.OBTanimoto;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.IndexUtilsFloat;
import net.obsearch.index.OBVectorFloat;
import net.obsearch.index.ghs.impl.Sketch64Long;
import net.obsearch.index.permprefix.impl.DistPermPrefixFloat;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.perm.impl.IncrementalPermFloat;
import net.obsearch.pivots.random.RandomPivotSelector;
import net.obsearch.pivots.rf03.RF03PivotSelectorLong;
import net.obsearch.query.OBQueryFloat;
import net.obsearch.query.OBQueryFloat;
import net.obsearch.query.OBQueryLong;
import net.obsearch.result.OBPriorityQueueFloat;
import net.obsearch.result.OBPriorityQueueLong;

public class VectorsDemoPrefix extends AbstractGHSExample {
	
	final static Random r = new Random();
	
	final static int VEC_SIZE = 30;

	public VectorsDemoPrefix (String[] args) throws IOException,
			OBStorageException, OBException, IllegalAccessException,
			InstantiationException, PivotsUnavailableException {
		super(args);
		
	}


	@Override
	protected void create() throws FileNotFoundException, OBStorageException,
			OBException, IOException, IllegalAccessException,
			InstantiationException, PivotsUnavailableException {
		// TODO Auto-generated method stub
		
		logger.info("Creating dataset :)");
		RandomPivotSelector<OBVectorFloat> sel = new RandomPivotSelector<OBVectorFloat>(
				new AcceptAll<OBVectorFloat>());
		//IncrementalPermFloat<OBVectorFloat> sel = new IncrementalPermFloat<OBVectorFloat>(new AcceptAll(), 400, 400);
		DistPermPrefixFloat<OBVectorFloat> index = new DistPermPrefixFloat<OBVectorFloat>(
				OBVectorFloat.class, sel, 1024, 0, 1024);
		
		// sel.setDesiredDistortion(0.10);
		// sel.setDesiredSpread(.70);
		// make the bit set as short so that m objects can fit in the buckets.
		
		index.setExpectedEP(.97f);
		index.setSampleSize(100);
		index.setKAlpha(alpha);
		// select the ks that the user will call.
		index.setMaxK(new int[] { 1 });
		index.setFixedRecord(true);
		index.setFixedRecord(VEC_SIZE * 4);
		// Create the ambient that will store the index's data. (NOTE: folder
		// name is hardcoded)
		// Ambient<L1, Sketch64Short<L1>> a = new AmbientBDBDb<L1,
		// Sketch64Short<L1>>( index, INDEX_FOLDER );
		// Ambient<L1, Sketch64Short<L1>> a = new AmbientMy<L1,
		// Sketch64Short<L1>>( index, INDEX_FOLDER );
		Ambient<OBVectorFloat, DistPermPrefixFloat<OBVectorFloat>> a = new AmbientTC<OBVectorFloat, DistPermPrefixFloat<OBVectorFloat>>(
				index, indexFolder);

		// Add some random objects to the index:
		logger.info("Adding " + super.databaseSize + " objects...");
		int i = 0;
		while (i < super.databaseSize) {
			index.insert(new OBVectorFloat(r, VEC_SIZE));
			if (i % 100000 == 0) {
				logger.info("Loading: " + i);
			}
			i++;
		}

		// prepare the index
		logger.info("Preparing the index...");
		a.freeze();
		logger.info("YAY! stats: " + index.getStats());
		// add the rest of the objects
		/*
		 * while(i < DB_SIZE){ index.insert(generateVector()); if(i % 100000 ==
		 * 0){ logger.info("Loading: " + i); } i++; }
		 */
		a.close();
	}

	
	
	protected void intrinsic() throws IllegalIdException, IllegalAccessException, InstantiationException, OBException, FileNotFoundException, IOException{
		

		Ambient<L1Long, Sketch64Long<L1Long>> a =  new AmbientTC<L1Long, Sketch64Long<L1Long>>(super.indexFolder);
		Sketch64Long<L1Long> index = a.getIndex();
		logger
				.info("Intrinsic dim: "
						+ index.intrinsicDimensionality(1000));
		
	}

	@Override
	protected void search() throws FileNotFoundException, OBStorageException,
			NotFrozenException, IllegalAccessException, InstantiationException,
			OBException, IOException {
		// TODO Auto-generated method stub
		
		Ambient<OBVectorFloat, DistPermPrefixFloat<OBVectorFloat>> a =  new AmbientTC<OBVectorFloat, DistPermPrefixFloat<OBVectorFloat>>(super.indexFolder);
		DistPermPrefixFloat<OBVectorFloat> index = a.getIndex();
		index.resetStats(); // reset the stats counter
		long start = System.currentTimeMillis();
		index.setKAlpha(1);
		List<OBQueryFloat> queryResults = new ArrayList<OBQueryFloat>(querySize);
		List<OBVectorFloat> queries = new ArrayList<OBVectorFloat>(super.querySize);
		int i = 0;
		
		logger.info("Warming cache...");
		index.bucketStats();	
		logger.info("Starting search!");
		index.resetStats();
		while(i < querySize){
			OBVectorFloat q = 	new OBVectorFloat(r, VEC_SIZE);
			// query the index with k=1			
			OBPriorityQueueFloat<OBVectorFloat> queue = new OBPriorityQueueFloat<OBVectorFloat>(1);			
			// perform a query with r=3000000 and k = 1 
			index.searchOB(q, Float.MAX_VALUE, queue);
			queryResults.add(new OBQueryFloat(q, queue.peek().getDistance(), queue));
			queries.add(q);
			logger.info("Doing query: " + i );
			i++;
		}
		// print the results of the set of queries. 
		long elapsed = System.currentTimeMillis() - start;
		logger.info("Time per query: " + elapsed / querySize + " millisec.");
		
		logger.info("Stats follow: (total distances / pivot vectors computed during the experiment)");
		logger.info(index.getStats().toString());

		
		logger.info("Doing CompoundError validation");
		StaticBin1D ep = new StaticBin1D();
		
		IndexUtilsFloat.validateResults(queryResults, index);
		
		
	}
	

	
	
	public static void main(String args[]) throws FileNotFoundException,
	OBStorageException, NotFrozenException, IllegalAccessException,
	InstantiationException, OBException, IOException,
	PivotsUnavailableException {

		VectorsDemoPrefix s = new VectorsDemoPrefix(args);

}

}
