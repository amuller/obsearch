package net.obsearch.example.doc;

import hep.aida.bin.StaticBin1D;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.kohsuke.args4j.CmdLineParser;

import net.obsearch.ambient.Ambient;
import net.obsearch.ambient.bdb.AmbientBDBDb;
import net.obsearch.ambient.bdb.AmbientBDBJe;
import net.obsearch.ambient.my.AmbientMy;
import net.obsearch.ambient.tc.AmbientTC;
import net.obsearch.example.AbstractExampleGeneral;
import net.obsearch.example.AbstractGHSExample;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.ghs.impl.Sketch64Float;

import net.obsearch.index.utils.Directory;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezFloat;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.pivots.rf03.RF03PivotSelectorFloat;
import net.obsearch.pivots.rf04.RF04PivotSelectorFloat;
import net.obsearch.pivots.rf02.RF02PivotSelectorFloat;
import net.obsearch.pivots.rf02.RF02PivotSelectorShort;
import net.obsearch.query.OBQueryFloat;
import net.obsearch.query.OBQueryShort;
import net.obsearch.result.OBPriorityQueueFloat;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;

public class WikipediaDemoRF04 extends AbstractGHSExample {

	public WikipediaDemoRF04(String args[]) throws IOException, OBStorageException, OBException, IllegalAccessException, InstantiationException, PivotsUnavailableException {
		super(args);

	}
	
	protected void search() throws OBStorageException, NotFrozenException, IllegalAccessException, InstantiationException, OBException, IOException{
		BufferedReader qData = new BufferedReader(new FileReader(query));
		Ambient<OBTanimoto, Sketch64Float<OBTanimoto>> a = new AmbientTC<OBTanimoto, Sketch64Float<OBTanimoto>>(
				indexFolder);
		Sketch64Float<OBTanimoto> index = a.getIndex();
		// now we can match some objects!
		logger.info("Querying the index...");
		int i = 0;
		index.resetStats();
		logger.info("Index stats: " + index.getStats().toString());
		index.setKAlpha(1f);
		long start = System.currentTimeMillis();
		List<OBPriorityQueueFloat<OBTanimoto>> queryResults = new ArrayList<OBPriorityQueueFloat<OBTanimoto>>(
				querySize);
		List<OBTanimoto> queries = new ArrayList<OBTanimoto>(querySize);
		String line = qData.readLine();
		logger.info("Warming cache...");
		index.bucketStats();
		index.resetStats(); // reset the stats counter
		logger.info("Search starts!");
		while (line != null && i < querySize) {
			OBTanimoto q = new OBTanimoto(line);
			line = qData.readLine();
			// query the index with k=1
			OBPriorityQueueFloat<OBTanimoto> queue = new OBPriorityQueueFloat<OBTanimoto>(
					1);
			// perform a query with r=3000000 and k = 1
			index.searchOB(q, Float.MAX_VALUE, queue);
			logger.info("Query: "
					+ q.getName()
					+ " found: "
					+ queue.getSortedElements().get(0).getObject()
							.getName() + " dist: "
					+ queue.getSortedElements().get(0).getDistance());
			queryResults.add(queue);
			queries.add(q);

			i++;
		}
		// print the results of the set of queries.
		long elapsed = System.currentTimeMillis() - start;
		logger
				.info("Time per query: " + elapsed / querySize
						+ " millisec.");

		logger
				.info("Stats follow: (total distances / pivot vectors computed during the experiment)");
		logger.info(index.getStats().toString());

		
		logger.info("Doing CompoundError validation");
		StaticBin1D ep = new StaticBin1D();

		Iterator<OBPriorityQueueFloat<OBTanimoto>> it1 = queryResults
				.iterator();
		Iterator<OBTanimoto> it2 = queries.iterator();
		StaticBin1D seqTime = new StaticBin1D();
		i = 0;
		while (it1.hasNext()) {
			OBPriorityQueueFloat<OBTanimoto> qu = it1.next();
			OBTanimoto q = it2.next();
			long time = System.currentTimeMillis();
			float[] sortedList = index.fullMatchLite(q, false);
			long el = System.currentTimeMillis() - time;
			seqTime.add(el);
			logger.info("Elapsed: " + el + " " + i);
			OBQueryFloat<OBTanimoto> queryObj = new OBQueryFloat<OBTanimoto>(
					q, Float.MAX_VALUE, qu, null);
			ep.add(queryObj.ep(sortedList));
			i++;
		}

		logger.info(ep.toString());
		logger.info("Time per seq query: ");
		logger.info(seqTime.toString());
		
	}
	
	protected void intrinsic() throws IllegalIdException, IllegalAccessException, InstantiationException, OBException, FileNotFoundException, IOException{
		Ambient<OBTanimoto, Sketch64Float<OBTanimoto>> a = new AmbientTC<OBTanimoto, Sketch64Float<OBTanimoto>>(
				indexFolder);
		Sketch64Float<OBTanimoto> index = a.getIndex();

		logger
				.info("Intrinsic dim: "
						+ index.intrinsicDimensionality(1000));
	}
	
	protected void create() throws OBStorageException, OBException, IOException, IllegalAccessException, InstantiationException, PivotsUnavailableException{
		BufferedReader dbData = new BufferedReader(new FileReader(database));

		// Create a pivot selection strategy
		// new AcceptAll<OBTanimoto>());
		RF04PivotSelectorFloat<OBTanimoto> sel = new RF04PivotSelectorFloat<OBTanimoto>(
				new AcceptAll<OBTanimoto>());/*
				
		sel.setDataSample(400);
		sel.setRepetitions(1000);
		sel.setDesiredDistortion(0.30f);
		sel.setDesiredSpread(0);
*/
		sel.setDataSample(400);
		// make the bit set as short so that m objects can fit in the
		// buckets.
/*		Sketch64Float<OBTanimoto> index = new Sketch64Float<OBTanimoto>(
				OBTanimoto.class, sel, 256, 0);
		*/
		Sketch64Float<OBTanimoto> index = new Sketch64Float<OBTanimoto>(OBTanimoto.class,
				sel, 64, 0);

		
		index.setExpectedEP(.95f);
		index.setSampleSize(100);
		// select the ks that the user will call.
		index.setMaxK(new int[] { 1 });

		// Create the ambient that will store the index's data.
		Ambient<OBTanimoto, Sketch64Float<OBTanimoto>> a = new AmbientTC<OBTanimoto, Sketch64Float<OBTanimoto>>(
				index, indexFolder);

		// Add some random objects to the index:
		logger.info("Adding " + databaseSize + " objects...");
		int i = 0;
		String line = dbData.readLine();
		while (line != null && i < databaseSize) {
			OBTanimoto o = new OBTanimoto(line);
			index.insert(o);
			line = dbData.readLine();
			if (i % 100000 == 0) {
				logger.info("Loading: " + i);
			}
			i++;
		}

		// prepare the index
		logger.info("Preparing the index...");
		a.freeze();
		// close the index (very important!)
		a.close();
	}	

	public static void main(String args[]) throws FileNotFoundException,
			OBStorageException, NotFrozenException, IllegalAccessException,
			InstantiationException, OBException, IOException,
			PivotsUnavailableException {

		WikipediaDemoRF04 g = new WikipediaDemoRF04(args);

	}

}
