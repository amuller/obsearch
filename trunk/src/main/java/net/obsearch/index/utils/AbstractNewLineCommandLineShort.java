package net.obsearch.index.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.Option;

import net.obsearch.AbstractOBResult;
import net.obsearch.ApproxIndexShort;
import net.obsearch.ambient.Ambient;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.IndexShort;
import net.obsearch.ob.OBShort;
import net.obsearch.query.AbstractOBQuery;
import net.obsearch.query.OBQueryShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultInvertedShort;
import net.obsearch.result.OBResultShort;
import net.obsearch.stats.Statistics;

/**
 * In this command line helper, data is separated by newlines and
 * Index and objects are of type short. 
 * @author Arnoldo Jose Muller-Molina
 *
 * @param <O> Object that we are handling
 * @param <I> The index that stores all data
 * @param <A> The ambient that controls the index.
 */
public abstract class AbstractNewLineCommandLineShort<O extends OBShort, I extends IndexShort<O>, A extends Ambient<O,I>> extends
		AbstractNewLineCommandLine<O, I, A> {
	
	private static Logger logger = Logger.getLogger(AbstractNewLineCommandLineShort.class);
	
	
	@Option(name = "-histogram", usage = "Generate histogram of distances")
	protected boolean histogram = false;
	
	@Option(name = "-histogramFile", usage = "Generate histogram of distances")
	protected File histogramFile = new File("histogram.csv");
	
	protected ArrayList<O> seq = null;
	
	protected void searchObjectApprox(I index, O object, Statistics other) throws NotFrozenException,
	IllegalIdException, OutOfRangeException, InstantiationException,
	IllegalAccessException, OBException, IOException {
		OBAsserts.chkAssert( index instanceof ApproxIndexShort, "Index must implement the interface " + ApproxIndexShort.class.getCanonicalName());
		OBAsserts.chkAssert(index.databaseSize() <= Integer.MAX_VALUE, "db is too large");		
		short perfectRange = (short)r;
		int perfectK;
		// k is different in ep mode (all the db) and in recall mode (k).
		if(mode == Mode.approxEvalEP){
			perfectK = (int)index.databaseSize();
		}else{
			perfectK = k;
		}

		// perform sequential search
		OBPriorityQueueShort<O> result = new OBPriorityQueueShort<O>(perfectK);
		OBQueryShort<O> dbQueue = new OBQueryShort<O>(object,perfectRange, result );
		List<OBResultShort<O>> results = new ArrayList<OBResultShort<O>>((int)index.databaseSize());
		
		int i = 0;
		int max = (int)index.databaseSize();
		if(seq == null){
			seq = new ArrayList<O>(max);
			while (i < max) {
				seq.add(index.getObject(i));
				i++;
			}
		}
		
		i = 0;
		for(O o : seq){
			short res = object.distance(o);          
			results.add(new OBResultShort<O>(o, i, res));
			i++;
		}
		       
        Collections.sort(results);
        Collections.reverse(results);
        // now we just have to ask the index to evaluate with the given ep or recall
        ApproxIndexShort<O> ai = (ApproxIndexShort<O>)index;
        OBPriorityQueueShort<O> res = new  OBPriorityQueueShort<O>(k);
        List<?> l =  results; // trick
        long distances = index.getStats().getDistanceCount();
        if(mode == Mode.approxEvalEP){
        	
        	ai.searchOBAnalyzeEp(object, (short)r, res, super.approxEvalEp, (List<AbstractOBResult<O>>)l);
        }else{
        	ai.searchOBAnalyzeRecall(object, (short)r, res, super.approxEvalRecall, (List<AbstractOBResult<O>>) l);
        }
        
        if(histogram){
        	long computedDistances = index.getStats().getDistanceCount() - distances;
        	short cost = res.getSortedElements().get(0).getDistance();
        	FileWriter f = new FileWriter(new File(histogramFile.getAbsolutePath() + "-" + k), true);
        	f.write(cost +  ", " + computedDistances +  "\n");
        	f.close();
        }
		
	}
	
	protected abstract Class<O> obtainClass();
	
	
	@Override
	protected void searchObject(I index, O object, Statistics other) throws NotFrozenException,
			IllegalIdException, OutOfRangeException, InstantiationException,
			IllegalAccessException, OBException, IOException {
		
		if(super.mode != Mode.x && mode != Mode.opt){
			index.resetStats();
		}
		OBPriorityQueueShort<O> result = new OBPriorityQueueShort<O>(k);
		short range = (short)r;
		long timeA = System.currentTimeMillis();		
		index.searchOB(object, range, result);	
		time += System.currentTimeMillis()- timeA;		
		//logger.info(result.toString() + " " + index.getStats().toStringSummary() + "time: " + time + " " + k + " " + r);
		other.incQueryCount();
		if(validate){
			IndexSmokeTUtilApprox<O> t = new IndexSmokeTUtilApprox<O>(null);
			ArrayList<OBResultShort<O>> x2 = new ArrayList<OBResultShort<O>>((int)index.databaseSize());
			t.searchSequential(index.databaseSize(), object, x2, index, range);
			
			double ep = t.ep(result, x2, index);			
			if(t.isApproxZero(result, x2, range)){
				other.incExtra("ZEROS");
			}else{
				other.addExtraStats("CompoundError", ep);			
			}
			if(! t.ok(result, x2, range)){
				other.incExtra("BAD");
			}
			other.addExtraStats("RECALL", t.recall(result, x2, k, range));

		}				
	}
	
}
