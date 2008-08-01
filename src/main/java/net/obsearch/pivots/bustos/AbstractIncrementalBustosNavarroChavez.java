package net.obsearch.pivots.bustos;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
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
public abstract class AbstractIncrementalBustosNavarroChavez < O extends OB >
        extends AbstractIncrementalPivotSelector < O > {

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractIncrementalBustosNavarroChavez.class);

    private int l;

    private int m;

    /**
     * Receives the object that accepts pivots as possible candidates. Selects l
     * pairs of objects to compare which set of pivots is better, and selects m
     * possible pivot candidates from the data set.
     * @param pivotable
     * @param l
     * @param m
     */
    protected AbstractIncrementalBustosNavarroChavez(Pivotable < O > pivotable,
            int l, int m) {
        super(pivotable);
        this.l = l;
        this.m = m;
    }
    
    /**
     * Resets the internal cache.
     */
    protected abstract void resetCache(int l);

    @Override
    public PivotResult generatePivots(int pivotCount, LongArrayList elements,
            Index < O > index) throws OBException, IllegalAccessException,
            InstantiationException, OBStorageException,
            PivotsUnavailableException {
        try {
            int lLocal = (int)Math.min(l, index.databaseSize());
            int mLocal = (int)Math.min(m, index.databaseSize());
            // do not process more than the amount of pivots in the DB.
            resetCache(lLocal);
            int max;
            if (elements == null) {
                max = (int)Math.min(index.databaseSize(), Integer.MAX_VALUE);
            } else {
                max = elements.size();
            }
            LongArrayList pivotList = new LongArrayList(pivotCount);
            Random r = new Random();
            // select m objects from which we will select pivots
            int i = 0;
            
            long[] x = select(lLocal, r, elements, index, null);
            long[] y = select(lLocal, r, elements, index, null);
            
            
            while (i < pivotCount) {
                long[] possiblePivots = select(mLocal, r, elements, index, pivotList);
                // select l pairs of objects to validate the pivots.
              
                // select the pivot in possiblePivots that maximizes the median
                // projected distances.
                logger.debug("Selecting pivot: " + i);
                long selectedPivot = selectPivot(pivotList, possiblePivots, x,
                        y, index);
                pivotList.add(selectedPivot);
                i++;
            }
            
          

            // return the pivots.
            pivotList.trimToSize();
            long[] result = pivotList.elements();
            // validate selected pivots.
            assert validatePivots(result, x[0], index);
            
            // we should reverse the result so that 
            // p1 has more pruning effect?
            
           /* int[] reversedResult = new int[result.length];
            i =0;
            int cx = result.length-1;
            while(i < result.length){
                reversedResult[i] = result[cx];
                cx--;
                i++;
            }*/
            
            return new PivotResult(result);
        } catch (DatabaseException d) {
            throw new OBStorageException(d);
        }

    }
    
    /**
     * Validates that the lower layers have been processing everything fine.
     * @param pivots the pivots that were selected
     * @param id Id of the object 
     * @param index Index from which we will load objects.
     */
    protected abstract boolean validatePivots(long[] pivots, long id, Index<O> index)throws DatabaseException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBException ;

    /**
     * Selects the best pivot based on the previousPivots and the possible set
     * of pivots
     * @param previousPivots
     *                All the pivots that have been selected
     * @param possiblePivots
     *                The possible set of pivots.
     * @param x
     *                (left item of the pair)
     * @param y
     *                (right item of the pair)
     * @return The best element in possiblePivots
     */
    private long selectPivot(LongArrayList previousPivots, long[] possiblePivots,
            long[] x, long[] y, Index < O > index) throws DatabaseException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException {
        double bestMedian = Double.NEGATIVE_INFINITY;
        long bestPivot = -1;
        previousPivots.trimToSize();
        long[] pivots = Arrays.copyOf(previousPivots.elements(), previousPivots.size() + 1);
        // initialize pivots.

        for (long pivotId : possiblePivots) {
            pivots[pivots.length - 1] = pivotId;
            double median = calculateMedian(pivots, x, y, index);
            if (median > bestMedian) {
                bestMedian = median;
                bestPivot = pivotId;
            }
        }
        // we have to calculate again the best pivot inside the cache.
        logger.debug("PivotMedian: " + bestMedian + " (pivot: " + bestPivot + ")");
        pivots[pivots.length - 1] = bestPivot;
        calculateMedian(pivots, x, y, index);
        return bestPivot;
    }

    /**
     * Calculates the median of L-inf(x[i], y[i]) based on pivots
     * @param pivots
     *                The pivots used to map the space
     * @param x
     *                The left part of the pair
     * @param y
     *                The right part of the pair.
     * @param index
     *                The underlying index (used to extract the objects and
     *                calculate the distances)
     */
    protected abstract double calculateMedian(long[] pivots, long[] x, long[] y,
            Index < O > index) throws DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException, OBException;   

}
