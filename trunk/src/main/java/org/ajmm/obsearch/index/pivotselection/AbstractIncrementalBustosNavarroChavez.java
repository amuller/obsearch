package org.ajmm.obsearch.index.pivotselection;

import java.util.Arrays;
import java.util.Random;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import cern.colt.list.IntArrayList;

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
            .getLogger(IncrementalKMeansPPPivotSelectorShort.class);

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

    @Override
    public int[] generatePivots(short pivotCount, IntArrayList elements,
            Index < O > index) throws OBException, IllegalAccessException,
            InstantiationException, OBStorageException,
            PivotsUnavailableException {
        try {

            int max;
            if (elements == null) {
                max = index.databaseSize();
            } else {
                max = elements.size();
            }
            IntArrayList pivotList = new IntArrayList(pivotCount);
            Random r = new Random();
            // select m objects from which we will select pivots
            int i = 0;
            while (i < pivotCount) {
                int[] possiblePivots = select(m, r, elements, index, pivotList);
                // select l pairs of objects to validate the pivots.
                int[] x = select(l, r, elements, index, null);
                int[] y = select(l, r, elements, index, null);
                // select the pivot in possiblePivots that maximizes the median
                // projected distances.
                logger.debug("Selecting pivot: " + i);
                int selectedPivot = selectPivot(pivotList, possiblePivots, x,
                        y, index);
                pivotList.add(selectedPivot);
                i++;
            }

            // return the pivots.
            pivotList.trimToSize();
            return pivotList.elements();
        } catch (DatabaseException d) {
            throw new OBStorageException(d);
        }

    }

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
    private int selectPivot(IntArrayList previousPivots, int[] possiblePivots,
            int[] x, int[] y, Index < O > index) throws DatabaseException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException {
        double bestMedian = Double.MIN_VALUE;
        int bestPivot = -1;
        previousPivots.trimToSize();
        int[] pivots = Arrays.copyOf(previousPivots.elements(), previousPivots
                .size() + 1);
        // initialize pivots.

        for (int pivotId : possiblePivots) {
            pivots[pivots.length - 1] = pivotId;
            double median = calculateMedian(pivots, x, y, index);
            if (median > bestMedian) {
                bestMedian = median;
                bestPivot = pivotId;
            }
        }
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
    protected abstract double calculateMedian(int[] pivots, int[] x, int[] y,
            Index < O > index) throws DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException, OBException;

    /**
     * Selects k random elements from the given source.
     * @param k
     *                number of elements to select
     * @param r
     *                Random object used to randomly select objects.
     * @param source
     *                The source of item ids.
     * @param index
     *                underlying index.
     * @param will not add pivots included in excludes.
     * @return The ids of selected objects.
     */
    private int[] select(int k, Random r, IntArrayList source, Index < O > index, IntArrayList excludes)
            throws OBStorageException, DatabaseException {
        int max = max(source, index);
        int[] res = new int[k];
        int i = 0;
        while (i < res.length) {
            int id = mapId(r.nextInt(max), source);
            if(excludes == null || ! excludes.contains(id)){
                res[i] = id;
            }else{
                continue; // repeat step.
            }
            i++;
        }
        return res;
    }   

}
