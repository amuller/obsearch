<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="IncrementalKMeansPPPivotSelector${Type}.java" />
package net.obsearch.pivots.kmeans.impl;

import java.util.Arrays;
import java.util.Random;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.utils.OBRandom;
import net.obsearch.pivots.AbstractIncrementalPivotSelector;
import net.obsearch.pivots.PivotResult;
import net.obsearch.pivots.Pivotable;

import net.obsearch.ob.OB${Type};
import org.apache.log4j.Logger;

import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;

import com.sleepycat.je.DatabaseException;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.   
 */
/**
 * This pivot selector uses k-means++ to select "good" centers.
 * @param <O>
 *            Type of object of the index to be analyzed.
 * @author Arnoldo Jose Muller Molina
 * @since 0.8
 */
<@gen_warning filename="IncrementalKMeansPPPivotSelector.java "/>
public class IncrementalKMeansPPPivotSelector${Type}<O extends OB${Type}> extends AbstractIncrementalPivotSelector<O>
         {

    private int retries = 3;
    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(IncrementalKMeansPPPivotSelector${Type}.class);

    /**
     * Creates a new IncrementalKMeansPPPivotSelector${Type} that will accept pivots
     * accepted by pivotable and will use index as the source of data.
     * @param index Index used to load and search objects
     * @param pivotable Object used to determine which objects are suitable for being pivots.
     */
    public IncrementalKMeansPPPivotSelector${Type}(Pivotable<O> pivotable){
        super(pivotable);
    }
    
    
    
    public PivotResult generatePivots(int pivotsCount, LongArrayList elements, Index<O> index) throws OBException,
    IllegalAccessException, InstantiationException, OBStorageException,
    PivotsUnavailableException
    {
        long centroidIds[] = null;
        try{
        // we need to prepare the index for freezing!
        int k = pivotsCount;
        double potential = 0;
        int databaseSize = max(elements,index);
        centroidIds = new long[k]; // keep track of the selected centroids
        ${type}[] closestDistances = new ${type}[databaseSize];
        OBRandom r = new OBRandom();
       
        // Randomly select one center
        int ind;
        int currentCenter = 0;
        O currentObject;
        do{
            ind = r.nextInt(databaseSize);
            centroidIds[currentCenter] = ind;
            currentObject = getObject(centroidIds[currentCenter], elements, index);
        }while(! pivotable.canBeUsedAsPivot(currentObject));
        
        int i = 0;
        while (i < databaseSize) {
            O toCompare = getObject(i, elements, index);
            closestDistances[i] = currentObject.distance( toCompare);
            potential += closestDistances[i];
            i++;
        }
        logger.debug("Found first pivot! " + Arrays.toString(centroidIds));

        // Choose the remaining k-1 centers
        int centerCount = 1;
        while (centerCount < k) {
            logger.debug("Finding pivot: " + centerCount + " : " + Arrays.toString(centroidIds));
            // Repeat several times
            double bestPotential = -1;
            int bestIndex = -1;
            for (int retry = 0; retry < retries; retry++) {

                // choose the new center
                double probability = r.nextFloat() * potential;
                O tempB = null; 
                for (ind = 0; ind < databaseSize ; ind++) {
                    if (contains(ind, centroidIds, centerCount)) {
                        continue;
                    }
                    if (probability <= closestDistances[ind]){
                        tempB = getObject(ind, elements, index);
                        if(pivotable.canBeUsedAsPivot(tempB)){
                            break;
                        }
                    }
                    
                    probability -= closestDistances[ind];
                }
                if(tempB == null){
                    throw new PivotsUnavailableException();
                }
                // Compute the new potential
                ${type} newPotential = 0;
                
                for (i = 0; i < databaseSize ; i++) {
                    if (contains(ind, centroidIds, centerCount)) {
                        continue;
                    }
                    O tempA = getObject(i, elements, index);
                    assert tempA != null;
                    assert tempB != null;
                    newPotential += Math.min(tempA.distance( tempB),
                            closestDistances[i]);
                }

                // Store the best result
                if (bestPotential < 0 || newPotential < bestPotential) {
                    bestPotential = newPotential;
                    bestIndex = ind;
                }
            }
            // make sure that the same center is not found
            assert !contains(bestIndex, centroidIds, centerCount);
            
            // store the new best index
            centroidIds[centerCount] = bestIndex;
            
            potential = bestPotential;
            O tempB = getObject(bestIndex,elements, index);
            for (i = 0; i < databaseSize; i++) {
                if (contains(ind, centroidIds, centerCount)) {
                    continue;
                }
                O tempA = getObject(i, elements, index);
                closestDistances[i] = (${type})Math.min(tempA.distance( tempB),
                        closestDistances[i]);
            }                        
            centerCount++;
        }        
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
        // store the pivots
        return new PivotResult(centroidIds);
    }
    
    /**
     * Returns true if id is in the array ids performs the operation up to max
     * (inclusive) if max is 0 this function always returns false.
     * @param id
     *            an identification
     * @param ids
     *            a list of numbers
     * @param max
     *            the maximum point that we will process
     * @return true if id is in the array ids
     */
    private boolean contains(final long id, final long[] ids, final int max) {
        int i = 0;
        if (max == 0) {
            return false;
        }
        while (i < ids.length && i <= max) {
            if (ids[i] == id) {
                return true;
            }
            i++;
        }
        return false;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

}
</#list>
