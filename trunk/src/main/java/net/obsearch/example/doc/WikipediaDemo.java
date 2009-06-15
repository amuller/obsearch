package net.obsearch.example.doc;

import hep.aida.bin.StaticBin1D;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.log4j.PropertyConfigurator;

import net.obsearch.ambient.Ambient;
import net.obsearch.ambient.bdb.AmbientBDBDb;
import net.obsearch.ambient.bdb.AmbientBDBJe;
import net.obsearch.ambient.my.AmbientMy;
import net.obsearch.ambient.tc.AmbientTC;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.ghs.impl.Sketch64Float;
import net.obsearch.index.ghs.impl.Sketch64Short;
import net.obsearch.index.utils.Directory;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.pivots.muller2.IncrementalMullerRosaShort;
import net.obsearch.pivots.rf02.RF02PivotSelectorFloat;
import net.obsearch.pivots.rf02.RF02PivotSelectorShort;
import net.obsearch.query.OBQueryFloat;
import net.obsearch.query.OBQueryShort;
import net.obsearch.result.OBPriorityQueueFloat;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;

public class WikipediaDemo  {
	
	final static File INDEX_FOLDER = new File("." + File.separator + "index");
	
	final static int DB_SIZE = 1000000;
	
	
	/**
	 * Query count.
	 */
	final static int QUERY_SIZE = 100;
	
	/**
	 * Logging provided by Java
	 */
	static Logger logger = Logger.getLogger(WikipediaDemo.class.getName());
	
public static void init() throws IOException{
		
		InputStream is = WikipediaDemo.class.getResourceAsStream(
				File.separator + "obsearch.properties");
		Properties props = new Properties();
		props.load(is);
		String prop = props.getProperty("log4j.file");
		PropertyConfigurator.configure(prop);
	}
	
	public static void main(String args[]) throws FileNotFoundException, OBStorageException, NotFrozenException, IllegalAccessException, InstantiationException, OBException, IOException, PivotsUnavailableException {
		
		File database = new File("/home/amuller/wikipedia/hope.txt.db");
		File query = new File("/home/amuller/wikipedia/hope.txt.q");
		BufferedReader dbData = new BufferedReader( new FileReader(database));
		BufferedReader qData = new BufferedReader( new FileReader(query));
		
		init();
		
		// Delete the directory of the index just in case.
		Directory.deleteDirectory(INDEX_FOLDER);
		
		
		// Create a pivot selection strategy for L1 distance
		 //IncrementalMullerRosaShort<L1> sel = new IncrementalMullerRosaShort<L1>(
	 	//			new AcceptAll<L1>(), 4000, 1000, (short) Short.MAX_VALUE);
		
		RF02PivotSelectorFloat<OBTanimoto> sel = new RF02PivotSelectorFloat<OBTanimoto>(new AcceptAll<OBTanimoto>());
		sel.setDataSample(1000);
		
		sel.setRepetitions(4000);
		sel.setDesiredDistortion(1);
		sel.setDesiredSpread(0);
		// make the bit set as short so that m objects can fit in the buckets.
	    Sketch64Float<OBTanimoto> index = new Sketch64Float<OBTanimoto>(OBTanimoto.class, sel, 64, 0);
	    index.setExpectedEP(0.0001);
	    index.setSampleSize(100);
	    
	    // select the ks that the user will call.
	    index.setMaxK(new int[]{1});	    
	    
		// Create the ambient that will store the index's data. (NOTE: folder name is hardcoded)
		//Ambient<L1, Sketch64Short<L1>> a =  new AmbientBDBDb<L1, Sketch64Short<L1>>( index, INDEX_FOLDER );
	    //Ambient<L1, Sketch64Short<L1>> a =  new AmbientMy<L1, Sketch64Short<L1>>( index, INDEX_FOLDER );
    	Ambient<OBTanimoto, Sketch64Float<OBTanimoto>> a =  new AmbientTC<OBTanimoto, Sketch64Float<OBTanimoto>>( index, INDEX_FOLDER );
		
		// Add some random objects to the index:	
		logger.info("Adding " + DB_SIZE + " objects...");
		int i = 0;	
		String line = dbData.readLine();
		while(line != null && i < DB_SIZE){
			index.insert(new OBTanimoto(line));
			line = dbData.readLine();
			if(i % 100000 == 0){
				logger.info("Loading: " + i);
			}
			i++;
		}
		
		// prepare the index
		logger.info("Preparing the index...");
		a.freeze();
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
		List<OBPriorityQueueFloat<OBTanimoto>> queryResults = new ArrayList<OBPriorityQueueFloat<OBTanimoto>>(QUERY_SIZE);
		List<OBTanimoto> queries = new ArrayList<OBTanimoto>(QUERY_SIZE);
		line = qData.readLine();
		while(line != null && i < QUERY_SIZE){
			OBTanimoto q = new OBTanimoto(line);
			line = qData.readLine();
			// query the index with k=1			
			OBPriorityQueueFloat<OBTanimoto> queue = new OBPriorityQueueFloat<OBTanimoto>(1);			
			// perform a query with r=3000000 and k = 1 
			index.searchOB(q, Float.MAX_VALUE, queue);
			logger.info("Query: " + q.getName() + " found: " + queue.getSortedElements().get(0).getObject().getName() + " dist: " + queue.getSortedElements().get(0).getDistance());
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
		

		Iterator<OBPriorityQueueFloat<OBTanimoto>> it1 = queryResults.iterator();
		Iterator<OBTanimoto> it2 = queries.iterator();
		StaticBin1D seqTime = new StaticBin1D();
		i = 0;
		while(it1.hasNext()){
			OBPriorityQueueFloat<OBTanimoto> qu = it1.next();
			OBTanimoto q = it2.next();
			long time = System.currentTimeMillis();
			float[] sortedList = index.fullMatchLite(q, false);
			long el = System.currentTimeMillis() - time;
			seqTime.add(el);
			logger.info("Elapsed: " + el + " "  + i);
			OBQueryFloat<OBTanimoto> queryObj = new OBQueryFloat<OBTanimoto>(q, Float.MAX_VALUE, qu, null);
			ep.add(queryObj.ep(sortedList));
			i++;
		}
		
		logger.info(ep.toString());
		logger.info("Time per seq query: ");
		logger.info(seqTime.toString());
	}

}
