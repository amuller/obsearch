package net.obsearch.pivots.sss;

import hep.aida.bin.StaticBin1D;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.pivots.AbstractIncrementalPivotSelector;
import net.obsearch.pivots.PivotResult;
import net.obsearch.pivots.Pivotable;
import net.obsearch.utils.Pair;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * IncrementalBustosNavarroChavez implements the pivot selection described here:
 * Pivot Selection Techniques for Proximity Searching in Metric Spaces (2001)
 * Benjamin Bustos, Gonzalo Navarro, Edgar Chavez The idea was also suggested by
 * Zezula et all in their book "Similarity Search: The Metric Space Approach"
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractMySSS < O extends OB >
        extends AbstractIncrementalPivotSelector < O > {

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractMySSS.class);

    /**
     * Alpha is the number of positive or negative STDS that we will add.
     */
    private double alpha = 0;
    /**
     * Sample to get a hitogram of distances
     */
    private int sample = 100;

    private StaticBin1D maxDistance = new StaticBin1D();
    /**
     * Receives the object that accepts pivots as possible candidates. Selects l
     * pairs of objects to compare which set of pivots is better, and selects m
     * possible pivot candidates from the data set.
     * @param pivotable
     * @param l
     * @param m
     */
    protected AbstractMySSS(Pivotable < O > pivotable) {
        super(pivotable);
    }
    
    public void setAlpha(double value){
    	this.alpha = value;
    }
    
    
    
    @Override
    public PivotResult generatePivots(int pivotCount, LongArrayList elements,
            Index < O > index) throws OBException, IllegalAccessException,
            InstantiationException, OBStorageException,
            PivotsUnavailableException {
    	long [] ids = super.select(sample, r, null, index, null);    	
    	long i = 0;
    	for(long id1 : ids){
    		logger.info("Doing: " + i + " mean: " + maxDistance.mean());
    		for(long id2 : ids){
    			if(id1 != id2){
    				O candidate1 = index.getObject(id1);
    				O candidate2 = index.getObject(id2);
    				maxDistance.add(distance(candidate1, candidate2));
    			}
    		}
    		i++;
    	}
    	logger.info("Sample histogram calculated, mean dist: " + maxDistance.mean());
    	
    	List<Pair<Long,O>> pivots = new LinkedList<Pair<Long,O>>();   
    	LongArrayList list = new LongArrayList(pivotCount);
    	while(pivots.size() < pivotCount){
    		boolean shouldAdd = true;
    		// add the pivot if it is sparsely separated
    		long id = super.select(1, r, elements, index, list)[0];
    		O candidate = index.getObject(id);
    		double max = current();
    		if(shouldAdd(candidate, pivots, false)){    			
    			pivots.add(new Pair<Long,O>(id, candidate));
    			list.add(id);
    			logger.info("Added candidate! " + pivots.size() + " pivots found!" + " max: " + maxDistance.mean() + " iter: " + i);
    		}
    		// if we changed the max, we have to cleanup
    		/*if(pivots.size() == (pivotCount - 1)){ //clean up 
    			List<Pair<Long,O>> toRemove = new LinkedList<Pair<Long,O>>();
    			for(Pair<Long,O> o : pivots){
    				if(! shouldAdd(o.getB(), pivots, true)){
    					toRemove.add(o);
    				}
    			}
    			pivots.removeAll(toRemove);
    			logger.info("Cleaning up! " + pivots.size() + " new max: " + maxDistance.mean());
    		}*/
    		i++;
    	}    
    	logger.info("Found: " + pivots.size() + " pivots");
    	// take 
    	return new PivotResult((List)pivots);

    }
    
    private double current(){
    	return maxDistance.mean() + (maxDistance.standardDeviation() * alpha);
    }
    
    private boolean shouldAdd(O candidate, List<Pair<Long,O>> pivots, boolean ignore) throws OBException{
    	boolean res = true;
    	for(Pair<Long,O> o : pivots){
    		if(ignore && o.getB().equals(candidate)){
    			continue;
    		}
			double dist = distance(o.getB(), candidate);
			maxDistance.add(dist);
			if(dist < current()){
				res = false;
				break;
			}
		}
    	return res;
    }
   protected abstract double distance(O o1, O o2) throws OBException;

}
