package net.obsearch.index.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.Option;

import net.obsearch.ambient.Ambient;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.IndexShort;
import net.obsearch.ob.OBShort;
import net.obsearch.result.OBPriorityQueueShort;
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
	
	
	
	@Override
	protected void searchObject(I index, O object, Statistics other) throws NotFrozenException,
			IllegalIdException, OutOfRangeException, InstantiationException,
			IllegalAccessException, OBException, IOException {
		
		if(super.mode != Mode.x){
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
			
			if(result.getSize() == 0 && x2.size() != 0){
				other.incExtra("ZEROS");
			}
			if(! t.ok(result, x2)){
				other.incExtra("BAD");
			}
			if(ep != 0){
				other.addExtraStats("EP", ep);				
			}
		}
		
		
	}

	

}
