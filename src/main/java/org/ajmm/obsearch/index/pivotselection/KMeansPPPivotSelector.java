package org.ajmm.obsearch.index.pivotselection;

import java.util.Arrays;
import java.util.Random;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.PivotSelector;
import org.apache.log4j.Logger;

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
public class KMeansPPPivotSelector < O extends OB > implements
        PivotSelector < O > {

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(KMeansPPPivotSelector.class);

    private Pivotable<O> pivotable;

    /**
     * Constructor of the pivot selection algorithm.
     * @param p
     *            A pivotable object that will tell us if we should use an
     *            object for pivot or not.
     */
    public KMeansPPPivotSelector(Pivotable<O> p) {
        pivotable = p;
    }

    /**
     * Generates n (n = pivots) from the database The method will modify the
     * pivot index and update the information of the selected new pivots
     * @param x
     *            Generate pivots from this index
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws PivotsUnavailableException
     *             If the pivots could not be found
     */
    public void generatePivots(final AbstractPivotIndex < O > x)
            throws OBException, IllegalAccessException, InstantiationException,
            DatabaseException, PivotsUnavailableException {
        // we need to prepare the index for freezing!
        x.prepareFreeze();
        short k = x.getPivotsCount();
        float potential = 0;
        int retries = 1;
        int databaseSize = x.databaseSize();
        int centroidIds[] = new int[k]; // keep track of the selected centroids
        float[] closestDistances = new float[databaseSize];
        Random r = new Random(System.currentTimeMillis());
       
        // Randomly select one center
        int index;
        int currentCenter;
        O currentObject;
        do{
            index = r.nextInt(databaseSize);
            currentCenter = 0;
            centroidIds[currentCenter] = index;
            currentObject = x.getObject(centroidIds[currentCenter]);
        }while(! pivotable.canBeUsedAsPivot(currentObject));
        
        int i = 0;
        while (i < databaseSize) {
            O toCompare = x.getObject(i);
            closestDistances[i] = x.distance(currentObject, toCompare);
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
                for (index = 0; index < databaseSize ; index++) {
                    if (contains(index, centroidIds, centerCount)) {
                        continue;
                    }
                    if (probability <= closestDistances[index]){
                        tempB = x.getObject(index);
                        if(pivotable.canBeUsedAsPivot(tempB)){
                            break;
                        }
                    }
                    
                    probability -= closestDistances[index];
                }

                // Compute the new potential
                float newPotential = 0;
                
                for (i = 0; i < databaseSize ; i++) {
                    if (contains(index, centroidIds, centerCount)) {
                        continue;
                    }
                    O tempA = x.getObject(i);
                    newPotential += Math.min(x.distance(tempA, tempB),
                            closestDistances[i]);
                }

                // Store the best result
                if (bestPotential < 0 || newPotential < bestPotential) {
                    bestPotential = newPotential;
                    bestIndex = index;
                }
            }
            // make sure that the same center is not found
            assert !contains(bestIndex, centroidIds, centerCount);
            
            // store the new best index
            centroidIds[centerCount] = bestIndex;
            
            potential = bestPotential;
            O tempB = x.getObject(bestIndex);
            for (i = 0; i < databaseSize; i++) {
                if (contains(index, centroidIds, centerCount)) {
                    continue;
                }
                O tempA = x.getObject(i);
                closestDistances[i] = Math.min(x.distance(tempA, tempB),
                        closestDistances[i]);
            }
            
            
            centerCount++;
        }
        
        // store the pivots
        x.storePivots(centroidIds);
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

}
