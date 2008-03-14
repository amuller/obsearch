package org.ajmm.obsearch.index.pivotselection;

import java.util.Arrays;
import java.util.Random;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.IncrementalPivotSelector;
import org.ajmm.obsearch.index.PivotSelector;
import org.ajmm.obsearch.index.utils.OBRandom;
import org.ajmm.obsearch.ob.OBShort;
import org.apache.log4j.Logger;

import cern.colt.list.IntArrayList;

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
public class IncrementalKMeansPPPivotSelectorShort<O extends OBShort> extends AbstractIncrementalPivotSelector<O> implements
        IncrementalPivotSelector {

    private int retries = 7;
    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(IncrementalKMeansPPPivotSelectorShort.class);

    /**
     * Creates a new IncrementalKMeansPPPivotSelectorShort that will accept pivots
     * accepted by pivotable and will use index as the source of data.
     * @param index Index used to load and search objects
     * @param pivotable Object used to determine which objects are suitable for being pivots.
     */
    public IncrementalKMeansPPPivotSelectorShort(Index<O> index, Pivotable<O> pivotable){
        this.index = index;
        this.pivotable = pivotable;
    }
    
    public int[] generatePivots(short pivotsCount) throws OBException,
    IllegalAccessException, InstantiationException, OBStorageException,
    PivotsUnavailableException
    {
        return generatePivots(pivotsCount,null);
    }
    
    public int[] generatePivots(short pivotsCount, IntArrayList elements) throws OBException,
    IllegalAccessException, InstantiationException, OBStorageException,
    PivotsUnavailableException
    {
        int centroidIds[] = null;
        try{
        // we need to prepare the index for freezing!
        short k = pivotsCount;
        float potential = 0;
        int databaseSize;
        if(elements == null){
            databaseSize = index.databaseSize();
        }else{
            databaseSize = elements.size();
        }
        centroidIds = new int[k]; // keep track of the selected centroids
        short[] closestDistances = new short[databaseSize];
        OBRandom r = new OBRandom();
       
        // Randomly select one center
        int ind;
        int currentCenter = 0;
        O currentObject;
        do{
            ind = r.nextInt(databaseSize);
            centroidIds[currentCenter] = ind;
            currentObject = getObject(centroidIds[currentCenter], elements);
        }while(! pivotable.canBeUsedAsPivot(currentObject));
        
        int i = 0;
        while (i < databaseSize) {
            O toCompare = getObject(i, elements);
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
            float bestPotential = -1;
            int bestIndex = -1;
            for (int retry = 0; retry < retries; retry++) {

                // choose the new center
                float probability = r.nextFloat() * potential;
                O tempB = null; 
                for (ind = 0; ind < databaseSize ; ind++) {
                    if (contains(ind, centroidIds, centerCount)) {
                        continue;
                    }
                    if (probability <= closestDistances[ind]){
                        tempB = getObject(ind, elements);
                        if(pivotable.canBeUsedAsPivot(tempB)){
                            break;
                        }
                    }
                    
                    probability -= closestDistances[ind];
                }

                // Compute the new potential
                short newPotential = 0;
                
                for (i = 0; i < databaseSize ; i++) {
                    if (contains(ind, centroidIds, centerCount)) {
                        continue;
                    }
                    O tempA = getObject(i, elements);
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
            O tempB = getObject(bestIndex,elements);
            for (i = 0; i < databaseSize; i++) {
                if (contains(ind, centroidIds, centerCount)) {
                    continue;
                }
                O tempA = getObject(i, elements);
                closestDistances[i] = (short)Math.min(tempA.distance( tempB),
                        closestDistances[i]);
            }                        
            centerCount++;
        }        
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
        // store the pivots
        return centroidIds;
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
    private boolean contains(final int id, final int[] ids, final int max) {
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
